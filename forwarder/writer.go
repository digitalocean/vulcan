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

package forwarder

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/digitalocean/vulcan/bus"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

const (
	namespace = "vulcan"
	subsystem = "forwarder"
)

// ErrServiceUnavailable returned when the Forwarder stops handling incoming requests
var ErrServiceUnavailable = errors.New("Forwarder Service Unavailable")

// Forwarder represents an object that accepts metrics from Prometheus.
// Metrics are grouped by target instance and written to the configured
// Vulcan message bus.
type Forwarder struct {
	writer bus.Writer

	wg   *sync.WaitGroup
	once *sync.Once
	done chan struct{}

	writeBatchDuration prometheus.Histogram
	reqTimeseriesCount prometheus.Histogram
}

// Config represents the configuration for a Forwarder.
type Config struct {
	Writer bus.Writer
}

// NewForwarder creates a new instance of Forwarder.
func NewForwarder(config *Config) *Forwarder {
	f := &Forwarder{
		writer: config.Writer,

		wg:   new(sync.WaitGroup),
		once: new(sync.Once),
		done: make(chan struct{}),

		writeBatchDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "write_batch_duration_seconds",
				Help:      "Duration of write batch calls.",
				Buckets:   prometheus.DefBuckets,
			},
		),
		reqTimeseriesCount: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "request_timeseries_length",
				Help:      "Number of timeseries objects in the write request.",
				Buckets:   []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			},
		),
	}

	f.wg.Add(1)

	return f
}

// Describe implements prometheus.Collector which makes the forwarder
// registrable to prometheus instrumentation.s
func (f *Forwarder) Describe(ch chan<- *prometheus.Desc) {
	f.writeBatchDuration.Describe(ch)
	f.reqTimeseriesCount.Describe(ch)
}

// Collect implements prometheus.Collector which makes the forwarder
// registrable to prometheus instrumentation.
func (f *Forwarder) Collect(ch chan<- prometheus.Metric) {
	f.writeBatchDuration.Collect(ch)
	f.reqTimeseriesCount.Collect(ch)
}

// Write implements remote.WriteServer interface.
func (f *Forwarder) Write(ctx context.Context, req *remote.WriteRequest) error {
	var (
		toWrite = map[string]*remote.WriteRequest{}
		ll      = log.WithFields(log.Fields{"source": "forwarder.Write"})
	)

	select {
	case <-f.done:
		return ErrServiceUnavailable

	default:
	}

	// Batch time series data by instance, fallback on address.
	for _, ts := range req.Timeseries {
		key, err := GetKey(ts)
		if err != nil {
			ll.WithFields(log.Fields{
				"timeseries": ts,
			}).WithError(err).Error("could not formulate key from labels")

			continue
		}

		if _, ok := toWrite[key]; !ok {
			toWrite[key] = &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{ts},
			}
		} else {
			toWrite[key].Timeseries = append(toWrite[key].Timeseries, ts)
		}
	}

	// Write each batch to the Vulcan bus.
	for key, wr := range toWrite {
		f.wg.Add(1)
		go func(key string, wr *remote.WriteRequest) {
			defer f.wg.Done()

			f.reqTimeseriesCount.Observe(float64(len(wr.Timeseries)))

			t0 := time.Now()
			if err := f.writer.Write(key, wr); err != nil {
				ll.WithError(err).Error("failed to write to bus")
			}
			f.writeBatchDuration.Observe(time.Since(t0).Seconds())
		}(key, wr)

	}

	return nil
}

// GetKey formulates the instance key based on Prometheus metric labels.
func GetKey(ts *remote.TimeSeries) (string, error) {
	var (
		jobName, inst string
		instFound     bool
	)

	for _, l := range ts.Labels {
		if jobName != "" && instFound {
			break
		}

		switch l.Name {
		case model.JobLabel:
			jobName = l.Value

		case model.InstanceLabel:
			inst = l.Value
			instFound = true

		case model.AddressLabel:
			if inst == "" {
				inst = l.Value
			}
		}
	}

	if jobName == "" {
		return "", errors.New("missing job label")
	}

	if inst == "" {
		return "", errors.New("missing instance label and address label")
	}

	return fmt.Sprintf("%s-%s", jobName, inst), nil
}

// Stop stops the frowarder service. All incoming requests are returned an
// ErrServiceUnavailable.
func (f *Forwarder) Stop() {
	f.once.Do(func() {
		close(f.done)
		f.wg.Done()
		f.wg.Wait()
	})
}
