package kafka

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/supershabam/pipeliner"
)

// Converter is an interface that wraps the Convert method.
type Converter interface {
	// Convert converts a Kafka consumer message to be processed by ingesters
	// and indexers.
	Convert(*sarama.ConsumerMessage) (bus.SampleGroup, error)
}

// DefaultConverter represents an oject has encapsulates the default Convert
// method.
type DefaultConverter struct{}

// Convert implements Converter.
func (dc DefaultConverter) Convert(msg *sarama.ConsumerMessage) (bus.SampleGroup, error) {
	return convert.PromTextToSG(bytes.NewReader(msg.Value))
}

// AckSource represents an object that processes SampleGroups received
// from the Kafka message bus.
type AckSource struct {
	Converter Converter

	ch  chan bus.AckPayload
	ctx pipeliner.Context
	err error

	sourceDurations *prometheus.SummaryVec
	errorsTotal     *prometheus.CounterVec
}

// AckSourceConfig represents the configuration of an AckSource object.
type AckSourceConfig struct {
	Addrs     []string
	ClientID  string
	Converter Converter
	Topic     string
}

// NewAckSource creates an instance of AckSource.
func NewAckSource(config *AckSourceConfig) (*AckSource, error) {
	as := &AckSource{
		Converter: config.Converter,
		sourceDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "ack_source",
				Name:      "duration_nanoseconds",
				Help:      "Durations of different ack_source stages",
			},
			[]string{"stage"},
		),
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vulcan",
				Subsystem: "ack_source",
				Name:      "errors_total",
				Help:      "Total number of errors of ack_source stages",
			},
			[]string{"stage"},
		),
	}
	err := as.run(config)
	if err != nil {
		return nil, err
	}
	return as, nil
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's sourceDurations and errorsTotal to the parameter ch.
func (as *AckSource) Describe(ch chan<- *prometheus.Desc) {
	as.sourceDurations.Describe(ch)
	as.errorsTotal.Describe(ch)
}

// Collect implements Collector.  Sends metrics collected by sourceDurations
// and errorsTotal to the parameter ch.
func (as *AckSource) Collect(ch chan<- prometheus.Metric) {
	as.sourceDurations.Collect(ch)
	as.errorsTotal.Collect(ch)
}

// Chan implements the bus.AckSource interface.  Returns the channel that feeds
// the message payload.
func (as *AckSource) Chan() <-chan bus.AckPayload {
	return as.ch
}

// Err implements the bus.AckSource interface.  Caller must call Err after the
// channel has been closed.  Will return the first error encountered.
func (as *AckSource) Err() error {
	return as.err
}

// Stop implements the bus.AckSource interface.  Closes the channel for payload
// processing.
func (as *AckSource) Stop() {
	as.ctx.Cancel(nil)
}

func (as *AckSource) run(config *AckSourceConfig) error {
	cfg := sarama.NewConfig()
	cfg.ClientID = config.ClientID
	consumer, err := sarama.NewConsumer(config.Addrs, cfg)
	if err != nil {
		return err
	}
	partitions, err := consumer.Partitions(config.Topic)
	if err != nil {
		return err
	}
	as.ch = make(chan bus.AckPayload)
	as.ctx = pipeliner.FirstError()
	go func() {
		defer close(as.ch)

		wg := sync.WaitGroup{}
		wg.Add(len(partitions))
		for _, partition := range partitions {
			go func(consumer sarama.Consumer, partition int32) {
				defer wg.Done()

				pconsumer, err := consumer.ConsumePartition(config.Topic, partition, sarama.OffsetNewest)
				if err != nil {
					as.ctx.Cancel(err)
					return
				}
				for {
					select {
					case <-as.ctx.Done():
						return
					case msg := <-pconsumer.Messages():
						t0 := time.Now()
						sg, err := as.Converter.Convert(msg)
						if err != nil {
							as.errorsTotal.WithLabelValues("convert").Add(1)
							log.Print(err)
							continue
						}
						as.sourceDurations.WithLabelValues("convert").Observe(float64(time.Since(t0).Nanoseconds()))
						payload := bus.AckPayload{
							Done:        func(error) {}, // NOOP ; TODO when using group consumer track offsets
							SampleGroup: sg,
						}
						select {
						case <-as.ctx.Done():
							return
						case as.ch <- payload:
						}
					}
				}
			}(consumer, partition)
		}
		wg.Wait()
	}()
	return nil
}
