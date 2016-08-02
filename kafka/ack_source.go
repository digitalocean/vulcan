package kafka

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/supershabam/pipeliner"
)

type Converter interface {
	Convert(*sarama.ConsumerMessage) (bus.SampleGroup, error)
}

type DefaultConverter struct{}

func (dc DefaultConverter) Convert(msg *sarama.ConsumerMessage) (bus.SampleGroup, error) {
	return convert.PromTextToSG(bytes.NewReader(msg.Value))
}

type AckSource struct {
	Converter Converter

	ch  chan bus.AckPayload
	ctx pipeliner.Context
	err error

	sourceDurations *prometheus.SummaryVec
	errorsTotal     *prometheus.CounterVec
}

type AckSourceConfig struct {
	Addrs     []string
	ClientID  string
	Converter Converter
	Topic     string
}

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

func (as AckSource) Describe(ch chan<- *prometheus.Desc) {
	as.sourceDurations.Describe(ch)
	as.errorsTotal.Describe(ch)
}

func (as AckSource) Collect(ch chan<- prometheus.Metric) {
	as.sourceDurations.Collect(ch)
	as.errorsTotal.Collect(ch)
}

func (as *AckSource) Chan() <-chan bus.AckPayload {
	return as.ch
}

func (as *AckSource) Err() error {
	return as.err
}

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
