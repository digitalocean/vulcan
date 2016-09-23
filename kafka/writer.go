// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/remote"
)

const (
	namespace = "vulcan"
	subsystem = "kafka"
)

var _ prometheus.Collector = &Writer{}

// Writer represents an object that encapsulates the behavior of a Kafka
// producter.
type Writer struct {
	producer sarama.AsyncProducer
	topic    string

	ch   chan *sarama.ProducerMessage
	done chan struct{}
	once *sync.Once

	successes, errors, enqueued int

	kafkaWriteStatus *prometheus.GaugeVec
	queuedForWrites  prometheus.Gauge
}

// NewWriter creates a new instance of Writer.
func NewWriter(config *WriterConfig) (*Writer, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = config.ClientID
	cfg.Producer.Compression = sarama.CompressionGZIP
	cfg.Producer.Return.Successes = config.TrackWrites

	producer, err := sarama.NewAsyncProducer(config.Addrs, cfg)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		producer: producer,
		topic:    config.Topic,

		ch:   make(chan *sarama.ProducerMessage, 1),
		done: make(chan struct{}),
		once: new(sync.Once),

		kafkaWriteStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "writes",
				Help:      "Count of kafka writes.",
			},
			[]string{"status"},
		),
		queuedForWrites: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queued_writes",
				Help:      "Number of messages in queued for writes to Kafka bus",
			},
		),
	}

	go w.run()

	return w, nil
}

// WriterConfig represents the configuration of a Writer object.
type WriterConfig struct {
	ClientID    string
	Addrs       []string
	Topic       string
	TrackWrites bool
}

// Describe implements prometheus.Collector which makes the forwarder
// registrable to prometheus instrumentation
func (w *Writer) Describe(ch chan<- *prometheus.Desc) {
	w.kafkaWriteStatus.Describe(ch)
	w.queuedForWrites.Describe(ch)
}

// Collect implements prometheus.Collector which makes the forwarder
// registrable to prometheus instrumentation
func (w *Writer) Collect(ch chan<- prometheus.Metric) {
	w.kafkaWriteStatus.Collect(ch)
	w.queuedForWrites.Collect(ch)
}

// Write sends metrics to the Kafka message bus.
func (w *Writer) Write(key string, req *remote.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	m := &sarama.ProducerMessage{
		Topic: w.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	w.ch <- m

	return nil
}

func (w *Writer) run() {
	var (
		wg sync.WaitGroup
		ll = log.WithFields(log.Fields{
			"source": "kafka.writer.run",
			"topic":  w.topic,
		})
	)

	// listen for successes
	wg.Add(1)
	go func() {
		defer wg.Done()

		for _ = range w.producer.Successes() {
			w.successes++
			w.enqueued--
			w.kafkaWriteStatus.WithLabelValues("success").Inc()
		}
	}()

	// listen for errors
	wg.Add(1)
	go func() {
		defer wg.Done()

		for err := range w.producer.Errors() {
			w.errors++
			w.enqueued--
			w.kafkaWriteStatus.WithLabelValues("error").Inc()

			ll.WithError(err).WithFields(log.Fields{
				"total_successes": w.successes,
				"total_errors":    w.errors,
				"total_queued":    w.enqueued,
			}).Error("write error to kafka received")
		}
	}()

	// listen for producer messages to write
ProducerLoop:
	for m := range w.ch {
		select {
		case w.producer.Input() <- m:
			ll.WithFields(log.Fields{
				"message_key":    m.Key,
				"message_length": m.Value.Length(),
			}).Debug("msg sent to queue")

			w.enqueued++
			w.queuedForWrites.Set(float64(w.enqueued))

		case <-w.done:
			w.producer.AsyncClose()
			ll.WithFields(log.Fields{
				"total_successes": w.successes,
				"total_errors":    w.errors,
				"total_queued":    w.enqueued,
			}).Debug("kafka writer stopped")

			break ProducerLoop
		}
	}

	wg.Wait()
}

// Stop gracefully stops the Kafka Writer
func (w *Writer) Stop() {
	w.once.Do(func() {
		close(w.done)
	})
}
