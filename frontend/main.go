package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Shopify/sarama"
	influx "github.com/influxdata/influxdb/models"
)

func main() {
	broker := os.Getenv("KAFKA_BROKERS")
	if broker == "" {
		panic("need $KAFKA_BROKER")
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := strings.Split(broker, ",")
	cl, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewAsyncProducerFromClient(cl)
	if err != nil {
		panic(err)
	}

	var (
		wg                          sync.WaitGroup
		enqueued, successes, errors int
	)

	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Printf("enq=%d succ=%d err=%d\n", enqueued, successes, errors)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	mux := http.NewServeMux()

	mux.Handle("/v1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enqueued++
		handleRequest(w, r, producer)
	}))

	srv := &http.Server{Addr: ":8086", Handler: mux}

	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("listen: %s\n", err)
		}
	}()

	<-signals // wait for SIGINT
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	srv.Shutdown(ctx)
	cancel()
	log.Println("HTTP done")
	producer.AsyncClose()

	log.Println("Kafka done")
	wg.Wait()
	log.Println("Server gracefully stopped")

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

const MaximumTags = 32
const MaximumAnnotations = 64
const MaxTagKeyLength = 64
const MaxTagValLength = 256
const MaxAnnKeyLength = 64
const MaxAnnValLength = 256
const MaxListCollections = 10000
const MaxCollectionLength = 256

var tagKeysRegex = regexp.MustCompile(`^[a-z][a-z0-9_.]+$`)
var annKeysRegex = tagKeysRegex

func isValidTagKey(k string) bool {
	return len(k) < MaxTagKeyLength && len(k) > 0 && tagKeysRegex.MatchString(k)
}
func isValidAnnKey(k string) bool {
	return len(k) < MaxAnnKeyLength && len(k) > 0 && annKeysRegex.MatchString(k)
}
func isValidTagValue(k string) bool {
	return len(k) < MaxTagValLength && len(k) > 0 && !bytes.Contains([]byte(k), []byte{0})
}
func isValidAnnotationValue(k string) bool {
	return len(k) < MaxAnnValLength
}
func isValidCollection(k string) bool {
	return (len(k) < MaxCollectionLength &&
		len(k) > 0 &&
		!bytes.Contains([]byte(k), []byte{0}) &&
		utf8.Valid([]byte(k)))
}

func handleRequest(w http.ResponseWriter, r *http.Request, p sarama.AsyncProducer) {
	msg := MetricBatch{
		Elements: []Metric{},
	}
	defer r.Body.Close()
	var pkey string
	// b, e := ioutil.ReadAll(r.Body)
	// fmt.Printf("r.Body is %s %v\n", string(b), e)
	rdr := bufio.NewReader(r.Body)
	ln, lerr := rdr.ReadString('\n')
	//fmt.Printf("read %q and %v\n", ln, err)
	linenum := 0
	errout := func(ln int, msg string) {
		w.WriteHeader(400)
		w.Write([]byte(fmt.Sprintf("error on line %d: %s\n", ln, msg)))
	}
	for lerr == nil {
		ipts, err := influx.ParsePointsString(ln)
		if err != nil {
			errout(linenum, err.Error())
			return
		}

		for _, p := range ipts {
			vals := make(map[string]float64)
			fi := p.FieldIterator()
			for fi.Next() {
				v, err := fi.FloatValue()
				if err != nil {
					errout(linenum, err.Error())
					return
				}
				vals[string(fi.FieldKey())] = v
			}
			tagz := p.Tags().Map()
			for tk, tv := range tagz {
				if !isValidTagKey(tk) || !isValidTagValue(tv) {
					errout(linenum, fmt.Sprintf("invalid BTrDB tag %q=%q\n", tk, tv))
					return
				}
			}
			if !isValidCollection(p.Name()) {
				errout(linenum, fmt.Sprintf("invalid BTrDB collection %q\n", p.Name()))
				return
			}
			pkey = p.Name()
			msg.Elements = append(msg.Elements, Metric{
				Collection: p.Name(),
				Tags:       p.Tags().Map(),
				Values:     vals,
				Timestamp:  p.Time().UnixNano(),
			})
		}

		ln, lerr = rdr.ReadString('\n')
		linenum++
	}
	fmt.Printf("finished loop linenum=%d error=%v\n", linenum, lerr)

	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf) // Will write to network.
	err := enc.Encode(&msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("wrote message batch with %d elements\n", len(msg.Elements))
	p.Input() <- &sarama.ProducerMessage{
		Topic: "ingressq.lineprotocol",
		Key:   sarama.StringEncoder(pkey),
		Value: sarama.ByteEncoder(buf.Bytes()),
	}
	w.Write([]byte("OK\n"))
}
