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
	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
	"github.com/gocql/gocql"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"

	log "github.com/Sirupsen/logrus"
)

const (
	metricNameKey model.LabelName = "__name__"
	magicPageSize                 = 120 // about 30 minutes of datapoints at 15s resolution = 30min * 60 seconds/min * 1 datapoint/15seconds
	magicPrefetch                 = 1.5 // should always have next page ready to go, and half-way through current page start getting the next-next page
)
const fetchUncompressedSQLIter = `SELECT at, value FROM uncompressed WHERE fqmn = ? AND at >= ? AND at <= ? ORDER BY at ASC`

// SeriesIterator enables efficient access of sample values in a series. Its
// methods are not goroutine-safe. A SeriesIterator iterates over a snapshot of
// a series, i.e. it is safe to continue using a SeriesIterator after or during
// modifying the corresponding series, but the iterator will represent the state
// of the series prior to the modification.
type SeriesIterator struct {
	iter       *gocql.Iter
	m          metric.Metric
	curr, last *model.SamplePair
	ready      chan struct{}
}

type SeriesIteratorConfig struct {
	Session       *gocql.Session
	Metric        metric.Metric
	After, Before model.Time
}

func NewSeriesIterator(config *SeriesIteratorConfig) (*SeriesIterator, error) {
	fqmn, err := convert.MetricToKey(toBusMetric(config.Metric.Metric))
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"fqmn": fqmn,
	}).Debug("new series iterator")
	si := &SeriesIterator{
		m: config.Metric,
		curr: &model.SamplePair{
			Timestamp: local.ZeroSamplePair.Timestamp,
			Value:     local.ZeroSamplePair.Value,
		},
		last: &model.SamplePair{
			Timestamp: local.ZeroSamplePair.Timestamp,
			Value:     local.ZeroSamplePair.Value,
		},
		ready: make(chan struct{}),
	}
	go func() {
		si.iter = config.Session.Query(fetchUncompressedSQLIter, fqmn, config.After, config.Before).PageSize(magicPageSize).Prefetch(magicPrefetch).Iter()
		close(si.ready)
	}()
	return si, nil
}

// ValueAtOrBeforeTime gets the value that is closest before the given time. In case a value
// exists at precisely the given time, that value is returned. If no
// applicable value exists, ZeroSamplePair is returned.
func (si *SeriesIterator) ValueAtOrBeforeTime(t model.Time) model.SamplePair {
	<-si.ready
	// curr == nil means that there are no more values to iterate over
	if si.curr == nil {
		return *si.last
	}
	if si.curr.Timestamp > t {
		return *si.last
	}
Read:
	si.last.Timestamp = si.curr.Timestamp
	si.last.Value = si.curr.Value
	if !si.iter.Scan(&si.curr.Timestamp, &si.curr.Value) {
		si.curr = nil // set curr to nil to signal no more values on iterator
		return *si.last
	}
	if si.curr.Timestamp < t {
		goto Read
	}
	return *si.last
}

// RangeValues gets all values contained within a given interval.
func (si *SeriesIterator) RangeValues(r metric.Interval) []model.SamplePair {
	return []model.SamplePair{}
}

// Metric returns the metric of the series that the iterator corresponds to.
func (si *SeriesIterator) Metric() metric.Metric {
	return si.m
}

// Close closes the iterator and releases the underlying data.
func (si *SeriesIterator) Close() {
	err := si.iter.Close()
	if err != nil {
		panic(err)
	}
	return
}

func toBusMetric(m model.Metric) bus.Metric {
	bm := bus.Metric{
		Name:   string(m[metricNameKey]),
		Labels: map[string]string{},
	}
	for k, v := range m {
		if k == metricNameKey {
			continue
		}
		bm.Labels[string(k)] = string(v)
	}
	return bm
}
