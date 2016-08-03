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

type ingester struct {
	prometheus.Collector

	sampleWriter storage.SampleWriter
	ackSource    bus.AckSource

	ingesterDurations *prometheus.SummaryVec
	errorsTotal       *prometheus.CounterVec
	work              chan workPayload
}

type IngesterConfig struct {
	SampleWriter storage.SampleWriter
	AckSource    bus.AckSource
}

func NewIngester(config *IngesterConfig) *ingester {
	i := &ingester{
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

func (i *ingester) worker() {
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

func (i ingester) Describe(ch chan<- *prometheus.Desc) {
	i.ingesterDurations.Describe(ch)
	i.errorsTotal.Describe(ch)
}

func (i ingester) Collect(ch chan<- prometheus.Metric) {
	i.ingesterDurations.Collect(ch)
	i.errorsTotal.Collect(ch)
}

func (i *ingester) Run() error {
	log.Println("running...")
	ch := i.ackSource.Chan()
	for payload := range ch {
		i.writeSampleGroup(payload.SampleGroup)
		payload.Done(nil)
	}
	return i.ackSource.Err()
}

func (i *ingester) writeSampleGroup(sg bus.SampleGroup) {
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
