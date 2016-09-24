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

package indexer

import (
	"sync"

	"github.com/digitalocean/vulcan/bus"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
)

// Indexer represents an object that consumes metrics from a message bus and
// writes them to indexing system.
type Indexer struct {
	prometheus.Collector

	Source        bus.Source
	SampleIndexer SampleIndexer

	numIndexGoroutines int

	once *sync.Once
	done chan struct{}

	indexDurations *prometheus.SummaryVec
	errorsTotal    *prometheus.CounterVec
}

// Config represents the configuration of an Indexer.  It takes an implmenter
// Acksource of the target message bus and an implmenter of SampleIndexer of
// the target indexing system.
type Config struct {
	Source             bus.Source
	SampleIndexer      SampleIndexer
	NumIndexGoroutines int
}

// NewIndexer creates a new instance of an Indexer.
func NewIndexer(config *Config) *Indexer {
	i := &Indexer{
		Source:             config.Source,
		SampleIndexer:      config.SampleIndexer,
		numIndexGoroutines: config.NumIndexGoroutines,

		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vulcan",
				Subsystem: "indexer",
				Name:      "errors_total",
				Help:      "Total number of errors of indexer stages",
			},
			[]string{"stage"},
		),

		once: new(sync.Once),
		done: make(chan struct{}),
	}

	return i
}

// Describe implements prometheus.Collector.  Sends decriptors of the
// instance's indexDurations, SampleIndexer, and errorsTotal to the parameter ch.
func (i *Indexer) Describe(ch chan<- *prometheus.Desc) {
	i.errorsTotal.Describe(ch)
	i.SampleIndexer.Describe(ch)
}

// Collect implements prometheus.Collector.  Sends metrics collected by the
// instance's indexDurations, SampleIndexer, and errorsTotal to the parameter ch.
func (i *Indexer) Collect(ch chan<- prometheus.Metric) {
	i.errorsTotal.Collect(ch)
	i.SampleIndexer.Collect(ch)
}

// Run starts the indexer process of consuming from the bus and indexing to
// the target indexing system.
func (i *Indexer) Run() error {
	var (
		wg       sync.WaitGroup
		writeErr error
	)

	log.Info("running")

	for n := 0; n < i.numIndexGoroutines; n++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := i.work(); err != nil {
				writeErr = err
				i.Done()
			}
		}()
	}
	wg.Wait()

	if writeErr != nil {
		return writeErr
	}
	return i.Source.Error()
}

func (i *Indexer) work() error {
	for {
		select {
		case m, ok := <-i.Source.Messages():
			if !ok {
				return nil
			}

			if err := i.SampleIndexer.IndexSamples(m.TimeSeriesBatch); err != nil {
				i.errorsTotal.WithLabelValues("index_sample").Add(1)

				return err
			}

			m.Ack()

		case <-i.done:
			return nil
		}
	}
}

// Done gracefully stops the indexer.
func (i *Indexer) Done() {
	i.once.Do(func() {
		close(i.done)
	})
}
