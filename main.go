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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

const KAFKATOPIC = "ingressq/"

func main() {
	broker := os.Getenv("KAFKA_BROKERS")
	if broker == "" {
		panic("need $KAFKA_BROKER")
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := strings.Split("broker", ",")
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

	mux.Handle("/lineprotocol", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	srv.Shutdown(ctx)
	log.Println("HTTP done")
	producer.AsyncClose()

	log.Println("Kafka done")
	wg.Wait()
	log.Println("Server gracefully stopped")

}

type Metric struct {
	Collection string
	Tags       map[string]string
	Values     map[string]float64
}
type MetricBatch struct {
	Elements []Metric
}

func handleRequest(w http.ResponseWriter, r *http.Request, p sarama.AsyncProducer) {
	msg := MetricBatch{
		Elements: []Metric{},
	}
	defer r.Body().Close()
	rdr := bufio.Reader(r.Body())
	ln, err := rdr.ReadString("\n")
	linenum = 0
	errout := func(ln int, msg string) {
		w.WriteHeader(400)
		w.Write([]byte(fmt.Sprintf("error on line %d: %s\n", ln, msg)))
	}
	for err == nil {
		parts = strings.Split(ln, " ")
		if len(parts) < 2 || len(parts) > 3 {
			errout(linenum, "invalid line")
			return
		}
		timestamp := time.Now().UnixNano()
		if len(parts) == 3 {
			ts, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				errout(linenum, "invalid timestamp")
				return
			}
			timestamp = ts
		}
		fparts := strings.Split(parts[0], ",")

		linenum++
	}
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf) // Will write to network.
	err := enc.Encode(&msg)
	if err != nil {
		panic(err)
	}
	p.Input() <- &sarama.ProducerMessage{
		Topic: KAFKATOPIC + "lineprotocol",
		Value: sarama.ByteEncoder(buf.Bytes()),
	}
}
