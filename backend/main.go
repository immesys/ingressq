package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	btrdb "gopkg.in/btrdb.v4"

	"github.com/Shopify/sarama"
	"github.com/pborman/uuid"
	"github.com/wvanbergen/kafka/consumergroup"
)

const tripLevel = 10000

var db *btrdb.BTrDB

func main() {
	broker := os.Getenv("KAFKA_BROKERS")
	if broker == "" {
		panic("need $KAFKA_BROKER")
	}

	var err error
	db, err = btrdb.Connect(context.Background(), btrdb.EndpointsFromEnv()...)
	if err != nil {
		panic(err)
	}
	consumer, consumerErr := consumergroup.JoinConsumerGroup(
		"ingressq",
		[]string{"ingressq.lineprotocol"},
		[]string{"zookeeper:2181"},
		nil)

	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		consumer.Close()
	}()

	tmt := time.After(10 * time.Second)
	eventq := make(chan []*sarama.ConsumerMessage, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for e := range eventq {
				flush(e)
			}
		}()
	}
	events := []*sarama.ConsumerMessage{}
MainLoop:
	for {
		select {
		case ev, ok := <-consumer.Messages():
			if !ok {
				break MainLoop
			}
			events = append(events, ev)
			if len(events) > tripLevel {
				eventq <- events
				consumer.CommitUpto(events[len(events)-1])
				events = []*sarama.ConsumerMessage{}
				tmt = time.After(10 * time.Second)
			}
		case <-tmt:
			if len(events) > 0 {
				eventq <- events
				consumer.CommitUpto(events[len(events)-1])
				events = []*sarama.ConsumerMessage{}
			}
			tmt = time.After(10 * time.Second)
		}
	}
}

//must panic on error
var streamcache map[ckey]*cval

type ckey struct {
	Collection string
	Tagstring  string
	Name       string
}

func (ck *ckey) SetTagstring(tags map[string]string) {
	tl := []string{}
	for k, v := range tags {
		tl = append(tl, k+"="+v+"$")
	}
	sort.StringSlice(tl).Sort()
	ck.Tagstring = strings.Join(tl, "")
}

type cval struct {
	S *btrdb.Stream
}

func init() {
	streamcache = make(map[ckey]*cval)

}

type Metric struct {
	Collection string
	Timestamp  int64
	Tags       map[string]string
	Values     map[string]float64
}
type MetricBatch struct {
	Elements []Metric
}

var scmu sync.Mutex
var createmu sync.Mutex

func flush(evz []*sarama.ConsumerMessage) {
	//Establish that the streams exist
	then := time.Now()
	scnt := 0
	pcnt := 0
	thisBatch := make(map[ckey][]btrdb.RawPoint)
	pfx := os.Getenv("INGRESSQ_PREFIX")
	fmt.Printf("flush called with %d events\n", len(evz))

	for _, msg := range evz {
		mb := MetricBatch{}
		dec := gob.NewDecoder(bytes.NewBuffer(msg.Value))
		err := dec.Decode(&mb)
		if err != nil {
			panic(err)
		}
		//fmt.Printf("event %d has %d metrics\n", msgi, len(mb.Elements))
		for _, m := range mb.Elements {
			for nm, vl := range m.Values {
				ck := ckey{Collection: m.Collection, Name: nm}
				ck.SetTagstring(m.Tags)
				scmu.Lock()
				_, ok := streamcache[ck]
				scmu.Unlock()
				if !ok {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					var s *btrdb.Stream
					streamTags := make(map[string]string)
					lookupTags := make(map[string]*string)
					for tk, tv := range m.Tags {
						tvc := tv
						lookupTags[tk] = &tvc
						streamTags[tk] = tv
					}
					lookupTags["name"] = &nm
					streamTags["name"] = nm
					coll := pfx + m.Collection
					//	fmt.Printf("doing lookup col=%q tags=%#v\n", coll, lookupTags)
                    createmu.Lock()
					cs, err := db.LookupStreams(ctx, coll, false, lookupTags, nil)
					if err != nil {
						panic(err)
					}
					if len(cs) == 0 {
						uu := uuid.NewRandom()
						m.Tags["name"] = nm
						unit, ok := m.Tags["unit"]
						if !ok {
							unit = "unknown"
						}
						//	fmt.Printf("doing create col=%q tags=%#v\n", coll, streamTags)
						news, err := db.Create(ctx, uu, coll, streamTags, btrdb.M{"unit": unit})
						if err != nil {
							panic(err)
						}
                        createmu.Unlock()
						s = news
					} else {
                        createmu.Unlock()
						s = cs[0]
					}
					cv := &cval{S: s}
					scmu.Lock()
					streamcache[ck] = cv
					scmu.Unlock()
				}
				//We now have CV
				lst := thisBatch[ck]
				lst = append(lst, btrdb.RawPoint{Time: m.Timestamp, Value: vl})
				thisBatch[ck] = lst
			} //end for loop over values
		} //end for loop over elements
	} //end for loop over events

	//Iterate over thisBatch and insert
	for ck, list := range thisBatch {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		scmu.Lock()
		stream := streamcache[ck]
		scmu.Unlock()
		ithen := time.Now()
		col, _ := stream.S.Collection(ctx)
		fmt.Printf("%s : Inserting %d into %s\n", ithen, len(list), col)
		err := stream.S.Insert(ctx, list)
		if err != nil {
			panic(err)
		}
		nw := time.Now()
		fmt.Printf("%s : insert done in %s\n", nw, nw.Sub(ithen))
		stream.S.Flush(ctx)
		nw2 := time.Now()
		fmt.Printf("%s : flush done in %s\n", nw2, nw2.Sub(nw))
		pcnt += len(list)
		scnt += 1
		cancel()
	}
	fmt.Printf("Inserted %d points in %d streams in %s\n", pcnt, scnt, time.Now().Sub(then))
}
