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

package storage

import (
	"sync"
	"time"

	"github.com/digitalocean/vulcan/indexer"
	"github.com/digitalocean/vulcan/model"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "vulcan"
	subsystem = "caching_indexer"
)

// CachingIndexer remembers when a metric was indexed last, if it is beyond
// a provided duration, then the provided Writer is called to write the
// metric and the time of the write is stored in CachingIndexer.
type CachingIndexer struct {
	indexer.SampleIndexer
	prometheus.Collector

	Indexer     indexer.SampleIndexer
	LastSeen    map[string]time.Time
	MaxDuration time.Duration
	m           sync.RWMutex

	indexDurations      *prometheus.SummaryVec
	indexBatchDurations prometheus.Histogram
}

// CachingIndexerConfig represents the configuration of a CachingIndexer object.
type CachingIndexerConfig struct {
	Indexer     indexer.SampleIndexer
	MaxDuration time.Duration
}

// NewCachingIndexer returns a new instance of the CachingIndexer.
func NewCachingIndexer(config *CachingIndexerConfig) *CachingIndexer {
	return &CachingIndexer{
		Indexer:     config.Indexer,
		LastSeen:    map[string]time.Time{},
		MaxDuration: config.MaxDuration,
		indexDurations: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "duration_seconds",
				Help:      "Durations of different caching_indexer stages",
			},
			[]string{"stage", "cache"},
		),
		indexBatchDurations: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "indexbatch_duration_seconds",
				Help:      "Duration of processing of an entire timeseries batch.",
				Buckets:   prometheus.DefBuckets,
			},
		),
	}
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's indexDurations and Indexer to the parameter ch.
func (ci *CachingIndexer) Describe(ch chan<- *prometheus.Desc) {
	ci.indexDurations.Describe(ch)
	ci.Indexer.Describe(ch)
	ci.indexBatchDurations.Describe(ch)
}

// Collect implements Collector.  Sends metrics collected by indexDurations
// and Indexer to the parameter ch.
func (ci *CachingIndexer) Collect(ch chan<- prometheus.Metric) {
	ci.indexDurations.Collect(ch)
	ci.Indexer.Collect(ch)
	ci.indexBatchDurations.Collect(ch)
}

// IndexSamples checks a entire timeseries batch to see if the samples are
// indexed.
func (ci *CachingIndexer) IndexSamples(tsb model.TimeSeriesBatch) error {
	t0 := time.Now()

	for _, ts := range tsb {
		if err := ci.IndexSample(ts); err != nil {
			return err
		}
	}
	ci.indexBatchDurations.Observe(time.Since(t0).Seconds())

	return nil
}

// IndexSample indexes the sample ts if the sample ts was not already indexed
// within the set MaxDuration.
func (ci *CachingIndexer) IndexSample(ts *model.TimeSeries) error {
	return ci.indexSample(ts, time.Now())
}

func (ci *CachingIndexer) indexSample(ts *model.TimeSeries, at time.Time) error {
	key := ts.ID()

	ci.m.RLock()
	last, ok := ci.LastSeen[key]
	ci.m.RUnlock()
	if ok && at.Sub(last) < ci.MaxDuration {
		ci.indexDurations.WithLabelValues("index_sample", "hit").Observe(
			time.Since(at).Seconds(),
		)

		return nil
	}

	if err := ci.Indexer.IndexSample(ts); err != nil {
		return err
	}

	ci.m.Lock()
	ci.LastSeen[key] = at
	ci.m.Unlock()

	ci.indexDurations.WithLabelValues("index_sample", "miss").Observe(
		float64(time.Since(at).Seconds()),
	)

	return nil
}
