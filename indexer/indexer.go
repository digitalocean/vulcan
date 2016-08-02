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

type indexer struct {
	prometheus.Collector
	Source        bus.AckSource
	SampleIndexer storage.SampleIndexer

	indexDurations *prometheus.SummaryVec
	errorsTotal    *prometheus.CounterVec
	work           chan workPayload
}

type IndexerConfig struct {
	Source        bus.AckSource
	SampleIndexer storage.SampleIndexer
}

func NewIndexer(config *IndexerConfig) *indexer {
	i := &indexer{
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

func (i indexer) Describe(ch chan<- *prometheus.Desc) {
	i.indexDurations.Describe(ch)
	i.errorsTotal.Describe(ch)
	i.SampleIndexer.Describe(ch)
}

func (i indexer) Collect(ch chan<- prometheus.Metric) {
	i.indexDurations.Collect(ch)
	i.errorsTotal.Collect(ch)
	i.SampleIndexer.Collect(ch)
}

func (i *indexer) Run() error {
	ch := i.Source.Chan()
	for payload := range ch {
		i.indexSampleGroup(payload.SampleGroup)
		payload.Done(nil)
	}
	return i.Source.Err()
}

func (i *indexer) indexSampleGroup(sg bus.SampleGroup) {
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

func (i *indexer) worker() {
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
