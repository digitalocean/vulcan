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

package cassandra

import (
	"fmt"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/model"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "vulcan"
	subsystem = "cassandra"
)

var writeTTLSampleCQL = `UPDATE %s USING TTL ? SET value = ? WHERE fqmn = ? AND at = ?`

// Writer implements ingester.Writer to persist TimeSeriesBatch samples
// to Cassandra.
type Writer struct {
	prometheus.Collector

	writeTTLSampleCQL string

	s          *gocql.Session
	ch         chan *writerPayload
	ttlSeconds int64

	batchWriteDuration  prometheus.Histogram
	sampleWriteDuration prometheus.Histogram
	workerCount         *prometheus.GaugeVec
}

// Describe implements prometheus.Collector.
func (w *Writer) Describe(ch chan<- *prometheus.Desc) {
	w.batchWriteDuration.Describe(ch)
	w.sampleWriteDuration.Describe(ch)
	w.workerCount.Describe(ch)
}

// Collect implements prometheus.Collector.
func (w *Writer) Collect(ch chan<- prometheus.Metric) {
	w.batchWriteDuration.Collect(ch)
	w.sampleWriteDuration.Collect(ch)
	w.workerCount.Collect(ch)
}

type writerPayload struct {
	wg    *sync.WaitGroup
	ts    *model.TimeSeries
	errch chan error
}

// WriterConfig specifies how many goroutines should be used in writing
// TimeSeries to Cassandra. The Session is expected to be already created
// and ready to use.
type WriterConfig struct {
	NumWorkers int
	Session    *gocql.Session
	TTL        time.Duration
	TableName  string
	Keyspace   string
}

// NewWriter creates a Writer and starts the configured number of
// goroutines to write to Cassandra concurrently.
func NewWriter(config *WriterConfig) *Writer {
	w := &Writer{
		s: config.Session,
		writeTTLSampleCQL: fmt.Sprintf(
			writeTTLSampleCQL,
			fmt.Sprintf("%s.%s", config.Keyspace, config.TableName),
		),

		ch:         make(chan *writerPayload),
		ttlSeconds: int64(config.TTL.Seconds()),

		batchWriteDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "batch_write_duration_seconds",
				Help:      "Histogram of seconds elapsed to write a batch.",
				Buckets:   prometheus.DefBuckets,
			},
		),
		sampleWriteDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "sample_write_duration_seconds",
				Help:      "Histogram of seconds elapsed to write a sample.",
				Buckets:   prometheus.DefBuckets,
			},
		),
		workerCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "worker_count",
				Help:      "Count of workers.",
			},
			[]string{"mode"},
		),
	}
	for n := 0; n < config.NumWorkers; n++ {
		go w.worker()
	}
	return w
}

// Write implements the ingester.Write interface and allows the
// ingester to write TimeSeriesBatch to Cassandra.
func (w *Writer) Write(tsb model.TimeSeriesBatch) error {
	t0 := time.Now()
	defer func() {
		w.batchWriteDuration.Observe(time.Since(t0).Seconds())
	}()
	wg := &sync.WaitGroup{}
	errch := make(chan error, 1) // room for just the first error a worker encounters
	wg.Add(len(tsb))
	for _, ts := range tsb {
		wp := &writerPayload{
			wg:    wg,
			ts:    ts,
			errch: errch,
		}
		w.ch <- wp
	}
	wg.Wait()
	select {
	case err := <-errch:
		return err
	default:
		return nil
	}
}

func (w *Writer) worker() {

	w.workerCount.WithLabelValues("idle").Inc()

	for m := range w.ch {
		w.workerCount.WithLabelValues("idle").Dec()
		w.workerCount.WithLabelValues("active").Inc()
		id := m.ts.ID()
		for _, s := range m.ts.Samples {
			err := w.write(id, s.TimestampMS, s.Value)
			if err != nil {
				// send error back on payload's errch; don't block the worker

				select {
				case m.errch <- err:
				default:
				}
			}
		}
		m.wg.Done()
		w.workerCount.WithLabelValues("idle").Inc()
		w.workerCount.WithLabelValues("active").Dec()
	}
}

func (w *Writer) write(id string, at int64, value float64) error {
	t0 := time.Now()
	defer func() {
		w.sampleWriteDuration.Observe(time.Since(t0).Seconds())
	}()
	return w.s.Query(w.writeTTLSampleCQL, w.ttlSeconds, value, id, at).Exec()
}
