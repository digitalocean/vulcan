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
	"github.com/digitalocean/vulcan/convert"
	"github.com/gocql/gocql"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"

	log "github.com/Sirupsen/logrus"
)

var fetchUncompressedSQLIter = `SELECT at, value FROM uncompressed WHERE fqmn = ? AND at >= ? AND at <= ? ORDER BY at ASC`

var _ local.SeriesIterator = &SeriesIterator{} // compile-time check that SeriesIterator implements local.SeriesIterator

// SeriesIterator is a Cassandra-backed implementation of a prometheus SeriesIterator.
type SeriesIterator struct {
	iter       *gocql.Iter
	fqmn       string
	m          metric.Metric
	curr, last *model.SamplePair
	list       []model.SamplePair
	ready      chan struct{}
}

// SeriesIteratorConfig is used in NewSeriesIterator to create a SeriesIterator.
type SeriesIteratorConfig struct {
	Session       *gocql.Session
	Metric        metric.Metric
	After, Before model.Time
	PageSize      int
	Prefetch      float64
}

// NewSeriesIterator creates a Cassandra-backed implementation of a prometheus storage
// SeriesIterator. This iterator immediately begins pre-fetching data upon creation.
func NewSeriesIterator(config *SeriesIteratorConfig) *SeriesIterator {
	ts := convert.MetricToTimeSeries(config.Metric)
	si := &SeriesIterator{
		m:    config.Metric,
		fqmn: ts.ID(),
		curr: &model.SamplePair{
			Timestamp: model.ZeroSamplePair.Timestamp,
			Value:     model.ZeroSamplePair.Value,
		},
		last: &model.SamplePair{
			Timestamp: model.ZeroSamplePair.Timestamp,
			Value:     model.ZeroSamplePair.Value,
		},
		list:  []model.SamplePair{},
		ready: make(chan struct{}),
	}
	// creating a gocql iterator takes time, so we instantiate it inside of a goroutine so
	// we can return quickly from NewSeriesIterator which is important for the performance
	// of the prometheus query engine. The si.ready channel signals when the iter is ready
	// to be used.
	go func() {
		si.iter = config.Session.Query(fetchUncompressedSQLIter, si.fqmn, config.After, config.Before).
			PageSize(config.PageSize).
			Prefetch(config.Prefetch).
			Iter()
		close(si.ready)
	}()
	return si
}

// ValueAtOrBeforeTime gets the value that is closest before the given time. In case a value
// exists at precisely the given time, that value is returned. If no
// applicable value exists, ZeroSamplePair is returned. This function assumes that
// ValueAtOrBeforeTime will be called only with incrementing values of t and that
// this SeriesIterator will only call either ValueAtOrBeforeTime or RangeValues, but
// not both functions.
func (si *SeriesIterator) ValueAtOrBeforeTime(t model.Time) model.SamplePair {
	<-si.ready
	// curr == nil means that there are no more values to iterate over.
	if si.curr == nil {
		return *si.last
	}
	for {
		if si.curr.Timestamp > t {
			return *si.last
		}
		si.last.Timestamp = si.curr.Timestamp
		si.last.Value = si.curr.Value
		ok := si.iter.Scan(&si.curr.Timestamp, &si.curr.Value)
		if !ok {
			// done iterating; set curr to nil to signal no more values on iter.
			si.curr = nil
			return *si.last
		}
	}
}

// RangeValues gets all values contained within a given interval. RangeValues assumes
// that the interval values OldestInclusive and NewestInclusive will always be
// higher than the previous call to RangeValues.
func (si *SeriesIterator) RangeValues(r metric.Interval) []model.SamplePair {
	<-si.ready
	// curr == nil means that there are no more values to iterate over.
	for si.curr != nil {
		ok := si.iter.Scan(&si.curr.Timestamp, &si.curr.Value)
		if !ok {
			si.curr = nil
			break
		}
		si.list = append(si.list, model.SamplePair{
			Timestamp: si.curr.Timestamp,
			Value:     si.curr.Value,
		})
		if si.curr.Timestamp > r.NewestInclusive {
			break
		}
	}
	// drop items in si.list that are older than OldestInclusive
	cropCount := 0
	for _, curr := range si.list {
		if curr.Timestamp >= r.OldestInclusive {
			break
		}
		cropCount++
	}
	si.list = si.list[cropCount:]
	// return portion of si.list that is older than NewestInclusive
	last := len(si.list)
	for ; last > 0; last-- {
		curr := si.list[last-1]
		if curr.Timestamp <= r.NewestInclusive {
			break
		}
	}
	return si.list[:last]
}

// Metric returns the metric of the series that the iterator corresponds to.
func (si *SeriesIterator) Metric() metric.Metric {
	return si.m
}

// Close closes the iterator and releases the underlying data.
func (si *SeriesIterator) Close() {
	<-si.ready
	err := si.iter.Close()
	if err != nil {
		log.WithError(err).WithField("fqmn", si.fqmn).Error("error while closing cassandra SeriesIterator")
	}
	return
}
