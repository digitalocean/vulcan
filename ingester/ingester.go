package ingester

import (
	"log"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/storage"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	numIngestGoroutines = 400
)

type workPayload struct {
	s  *bus.Sample
	wg *sync.WaitGroup
}

// Ingester represents an object that consumes metrics from a bus and writes
// them to a data storage.
type Ingester struct {
	prometheus.Collector

	sampleWriter storage.SampleWriter
	ackSource    bus.AckSource

	ingesterDurations *prometheus.SummaryVec
	errorsTotal       *prometheus.CounterVec
	work              chan workPayload
}

// Config represents the configuration of an Ingester.  It requires an
// AckSource implementer for the target message bus and a SampleWriter
// implementer of the data storage system.
type Config struct {
	SampleWriter storage.SampleWriter
	AckSource    bus.AckSource
}

// NewIngester creates a new instance of Ingester.
func NewIngester(config *Config) *Ingester {
	i := &Ingester{
		sampleWriter: config.SampleWriter,
		ackSource:    config.AckSource,
		ingesterDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "ingester",
				Name:      "duration_nanoseconds",
				Help:      "Durations of ingester stages",
			},
			[]string{"stage"},
		),
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vulcan",
				Subsystem: "ingester",
				Name:      "errors_total",
				Help:      "Count of errors total of ingester stages",
			},
			[]string{"stage"},
		),
		work: make(chan workPayload),
	}
	for n := 0; n < numIngestGoroutines; n++ {
		go i.worker()
	}
	return i
}

func (i *Ingester) worker() {
	for w := range i.work {
		t0 := time.Now()
		err := i.sampleWriter.WriteSample(w.s)
		w.wg.Done()
		if err != nil {
			log.Println(err)
			i.errorsTotal.WithLabelValues("write_sample").Add(1)
			continue
		}
		i.ingesterDurations.WithLabelValues("write_sample").Observe(float64(time.Since(t0).Nanoseconds()))
	}
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's ingesterDurations and errorsTotal to the parameter ch.
func (i *Ingester) Describe(ch chan<- *prometheus.Desc) {
	i.ingesterDurations.Describe(ch)
	i.errorsTotal.Describe(ch)
}

// Collect implements Collector.  Sends metrics collected by ingesterDurations
// and errorsTotal to the parameter ch.
func (i *Ingester) Collect(ch chan<- prometheus.Metric) {
	i.ingesterDurations.Collect(ch)
	i.errorsTotal.Collect(ch)
}

// Run starts the ingesting process by consuming from the message bus and
// writing to the data storage system.
func (i *Ingester) Run() error {
	log.Println("running...")
	ch := i.ackSource.Chan()
	for payload := range ch {
		i.writeSampleGroup(payload.SampleGroup)
		payload.Done(nil)
	}
	return i.ackSource.Err()
}

func (i *Ingester) writeSampleGroup(sg bus.SampleGroup) {
	t0 := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(len(sg))
	for _, s := range sg {
		i.work <- workPayload{
			s:  s,
			wg: wg,
		}
	}
	wg.Wait()
	i.ingesterDurations.WithLabelValues("write_sample_group").Observe(float64(time.Since(t0).Nanoseconds()))
}
