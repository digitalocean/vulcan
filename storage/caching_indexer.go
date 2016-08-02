package storage

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
)

// CachingIndexer remembers when a metric was indexed last, if it is beyond
// a provided duration, then the provided Writer is called to write the
// metric and the time of the write is stored in CachingIndexer.
type CachingIndexer struct {
	SampleIndexer

	Indexer     SampleIndexer
	LastSeen    map[string]time.Time
	MaxDuration time.Duration
	m           sync.RWMutex

	indexDurations *prometheus.SummaryVec
}

type CachingIndexerConfig struct {
	Indexer     SampleIndexer
	MaxDuration time.Duration
}

func NewCachingIndexer(config *CachingIndexerConfig) *CachingIndexer {
	return &CachingIndexer{
		Indexer:     config.Indexer,
		LastSeen:    map[string]time.Time{},
		MaxDuration: config.MaxDuration,
		indexDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "vulcan",
				Subsystem: "caching_indexer",
				Name:      "duration_nanoseconds",
				Help:      "Durations of different caching_indexer stages",
			},
			[]string{"stage", "cache"},
		),
	}
}

func (ci CachingIndexer) Describe(ch chan<- *prometheus.Desc) {
	ci.indexDurations.Describe(ch)
	ci.Indexer.Describe(ch)
}

func (ci CachingIndexer) Collect(ch chan<- prometheus.Metric) {
	ci.indexDurations.Collect(ch)
	ci.Indexer.Collect(ch)
}

func (ci *CachingIndexer) IndexSample(s *bus.Sample) error {
	return ci.indexSample(s, time.Now())
}

func (ci *CachingIndexer) indexSample(s *bus.Sample, at time.Time) error {
	t0 := time.Now()
	key, err := convert.MetricToKey(s.Metric)
	if err != nil {
		return err
	}
	ci.m.RLock()
	last, ok := ci.LastSeen[key]
	ci.m.RUnlock()
	if ok && at.Sub(last) < ci.MaxDuration {
		ci.indexDurations.WithLabelValues("index_sample", "hit").Observe(float64(time.Since(t0).Nanoseconds()))
		return nil
	}
	err = ci.Indexer.IndexSample(s)
	if err != nil {
		return err
	}
	ci.m.Lock()
	ci.LastSeen[key] = at
	ci.m.Unlock()
	ci.indexDurations.WithLabelValues("index_sample", "miss").Observe(float64(time.Since(t0).Nanoseconds()))
	return nil
}
