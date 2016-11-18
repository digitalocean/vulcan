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

package cacher

import (
	"github.com/digitalocean/vulcan/convert"
	"github.com/prometheus/client_golang/prometheus"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

// IteratorFactory is able to create SeriesIterators without the caller knowning the
// implementation.
type IteratorFactory struct {
	cfg    *IteratorFactoryConfig
	errors prometheus.Counter
}

// IteratorFactoryConfig is necessary to create a new IteratorFactory.
type IteratorFactoryConfig struct {
	Session *Session
}

// NewIteratorFactory creates and starts the background process for an IteratorFactory.
func NewIteratorFactory(cfg *IteratorFactoryConfig) (*IteratorFactory, error) {
	itrf := &IteratorFactory{
		cfg: cfg,
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "vulcan",
			Subsystem: "cacher_iterator",
			Name:      "errors_total",
			Help:      "Count of errors encountered while iterating",
		}),
	}
	return itrf, nil
}

// Describe implements prometheus.Collector.
func (itrf *IteratorFactory) Describe(ch chan<- *prometheus.Desc) {
	itrf.errors.Describe(ch)
}

// Collect implements prometheus.Collector.
func (itrf *IteratorFactory) Collect(ch chan<- prometheus.Metric) {
	itrf.errors.Collect(ch)
}

// Iterator returns a new SeriesIterator.
func (itrf *IteratorFactory) Iterator(m metric.Metric, from, through pmodel.Time) (local.SeriesIterator, error) {
	ts := convert.MetricToTimeSeries(m)
	id := ts.ID()
	return NewSeriesIterator(&SeriesIteratorConfig{
		ID:      id,
		Metric:  m,
		After:   int64(from),
		Before:  int64(through),
		Session: itrf.cfg.Session,
		Errors:  itrf.errors,
	}), nil
}
