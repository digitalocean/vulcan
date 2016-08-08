package indexer

import (
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/storage"
)

const (
	numIndexGoroutines = 400
)

type workPayload struct {
	s  *bus.Sample
	wg *sync.WaitGroup
}

// Indexer represents an object that consumes metrics from a message bus and
// writes them to indexing system.
type Indexer struct {
	prometheus.Collector
	Source        bus.AckSource
	SampleIndexer storage.SampleIndexer

	indexDurations *prometheus.SummaryVec
	errorsTotal    *prometheus.CounterVec
	work           chan workPayload
}

// Config represents the configuration of an Indexer.  It takes an implmenter
// Acksource of the target message bus and an implmenter of SampleIndexer of
// the target indexing system.
type Config struct {
	Source        bus.AckSource
	SampleIndexer storage.SampleIndexer
}

// NewIndexer creates a new instance of an Indexer.
func NewIndexer(config *Config) *Indexer {
	i := &Indexer{
		Source:        config.Source,
		SampleIndexer: config.SampleIndexer,

		indexDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "indexer",
				Name:      "duration_nanoseconds",
				Help:      "Durations of different indexer stages",
			},
			[]string{"stage"},
		),
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vulcan",
				Subsystem: "indexer",
				Name:      "errors_total",
				Help:      "Total number of errors of indexer stages",
			},
			[]string{"stage"},
		),
		work: make(chan workPayload),
	}
	for n := 0; n < numIndexGoroutines; n++ {
		go i.worker()
	}
	return i
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's indexDurations, SampleIndexer, and errorsTotal to the parameter ch.
func (i *Indexer) Describe(ch chan<- *prometheus.Desc) {
	i.indexDurations.Describe(ch)
	i.errorsTotal.Describe(ch)
	i.SampleIndexer.Describe(ch)
}

// Collect implements prometheus.Collector.  Sends metrics collected by the
// instance's indexDurations, SampleIndexer, and errorsTotal to the parameter ch.
func (i *Indexer) Collect(ch chan<- prometheus.Metric) {
	i.indexDurations.Collect(ch)
	i.errorsTotal.Collect(ch)
	i.SampleIndexer.Collect(ch)
}

// Run starts the indexer process of consuming from the bus and indexing to
// the target indexing system.
func (i *Indexer) Run() error {
	ch := i.Source.Chan()
	for payload := range ch {
		i.indexSampleGroup(payload.SampleGroup)
		payload.Done(nil)
	}
	return i.Source.Err()
}

func (i *Indexer) indexSampleGroup(sg bus.SampleGroup) {
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
	i.indexDurations.WithLabelValues("index_sample_group").Observe(float64(time.Since(t0).Nanoseconds()))
}

func (i *Indexer) worker() {
	for w := range i.work {
		t0 := time.Now()
		err := i.SampleIndexer.IndexSample(w.s)
		w.wg.Done()
		if err != nil {
			log.Println(err)
			i.errorsTotal.WithLabelValues("index_sample").Add(1)
			continue
		}
		i.indexDurations.WithLabelValues("index_sample").Observe(float64(time.Since(t0).Nanoseconds()))
	}
}
