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

// Writer represents an object that encapsulates the behavior of a Kafka
// producter.
type Writer struct {
	producer sarama.AsyncProducer
	topic    string

	done chan struct{}
	once *sync.Once
	wg   *sync.WaitGroup

	kafkaWriteStatus *prometheus.CounterVec
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

		done: make(chan struct{}),
		once: new(sync.Once),
		wg:   new(sync.WaitGroup),

		kafkaWriteStatus: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "message_writes_total",
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

	go func() {
		w.producer.Input() <- m

		w.queuedForWrites.Inc()
	}()

	return nil
}

func (w *Writer) run() {
	ll := log.WithFields(log.Fields{
		"source": "kafka.writer.run",
		"topic":  w.topic,
	})

	// listen for successes
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for _ = range w.producer.Successes() {
			w.queuedForWrites.Dec()
			w.kafkaWriteStatus.WithLabelValues("success").Inc()
		}
	}()

	// listen for errors
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for err := range w.producer.Errors() {
			w.queuedForWrites.Dec()
			w.kafkaWriteStatus.WithLabelValues("error").Inc()

			ll.WithError(err).Error("write error to kafka received")
		}
	}()

	// Add one more wait group to ensure clean up code gets executed,
	// then block until explicit service stop
	w.wg.Add(1)
	<-w.done

	w.producer.AsyncClose()
	ll.Warn("kafka writer stopped")

	w.wg.Done()
}

// Stop gracefully stops the Kafka Writer
func (w *Writer) Stop() {
	w.once.Do(func() {
		close(w.done)
		w.wg.Wait()
	})
}
