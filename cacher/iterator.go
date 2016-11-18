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
	"context"

	"github.com/prometheus/client_golang/prometheus"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
)

// SeriesIterator retreives chunks from a CacherClient and iterates through them implementing the
// prometheus local.SeriesIterator interface.
type SeriesIterator struct {
	cfg        *SeriesIteratorConfig
	chunkIter  chunk.Iterator
	curr, last *pmodel.SamplePair
	idx        int
	list       []pmodel.SamplePair
	ready      chan struct{}
	raw        [][]byte
}

// SeriesIteratorConfig is used in NewSeriesIterator to create a SeriesIterator.
type SeriesIteratorConfig struct {
	ID            string
	Metric        metric.Metric
	After, Before int64
	Session       *Session
	Errors        prometheus.Counter
}

// NewSeriesIterator creates the iterator.
func NewSeriesIterator(cfg *SeriesIteratorConfig) *SeriesIterator {
	si := &SeriesIterator{
		cfg: cfg,
		curr: &pmodel.SamplePair{
			Timestamp: pmodel.ZeroSamplePair.Timestamp,
			Value:     pmodel.ZeroSamplePair.Value,
		},
		last: &pmodel.SamplePair{
			Timestamp: pmodel.ZeroSamplePair.Timestamp,
			Value:     pmodel.ZeroSamplePair.Value,
		},
		list:  []pmodel.SamplePair{},
		ready: make(chan struct{}),
	}
	// fetch in the background since prometheus expects creation of SeriesIterator to return
	// without blocking.
	go si.fetch()
	return si
}

func (si *SeriesIterator) fetch() {
	defer close(si.ready)
	req := &ChunksRequest{
		Id:    si.cfg.ID,
		After: si.cfg.After,
	}
	resp, err := si.cfg.Session.Chunks(context.Background(), req)
	if err != nil {
		si.cfg.Errors.Inc()
		return
	}
	si.raw = resp.Chunks
}

// ValueAtOrBeforeTime gets the value that is closest before the given time. In case a value
// exists at precisely the given time, that value is returned. If no
// applicable value exists, ZeroSamplePair is returned. This function assumes that
// ValueAtOrBeforeTime will be called only with incrementing values of t and that
// this SeriesIterator will only call either ValueAtOrBeforeTime or RangeValues, but
// not both functions.
func (si *SeriesIterator) ValueAtOrBeforeTime(t pmodel.Time) pmodel.SamplePair {
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
		ts, v, ok := si.next()
		if !ok {
			// done iterating; set curr to nil to signal no more values on iter.
			si.curr = nil
			return *si.last
		}
		si.curr.Timestamp = ts
		si.curr.Value = v
	}
}

// RangeValues gets all values contained within a given interval. RangeValues assumes
// that the interval values OldestInclusive and NewestInclusive will always be
// higher than the previous call to RangeValues.
func (si *SeriesIterator) RangeValues(r metric.Interval) []pmodel.SamplePair {
	<-si.ready
	// curr == nil means that there are no more values to iterate over.
	for si.curr != nil {
		ts, v, ok := si.next()
		if !ok {
			si.curr = nil
			break
		}
		si.curr.Timestamp = ts
		si.curr.Value = v
		si.list = append(si.list, pmodel.SamplePair{
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
	return si.cfg.Metric
}

// Close closes the iterator and releases the underlying data.
func (si *SeriesIterator) Close() {
	<-si.ready
	return
}

func (si *SeriesIterator) next() (pmodel.Time, pmodel.SampleValue, bool) {
	if si.raw == nil {
		return 0, 0, false
	}
Start:
	if si.chunkIter == nil {
		if si.idx >= len(si.raw) {
			return 0, 0, false
		}
		chnk, err := chunk.NewForEncoding(chunk.Varbit)
		if err != nil {
			return 0, 0, false
		}
		err = chnk.UnmarshalFromBuf(si.raw[si.idx])
		if err != nil {
			return 0, 0, false
		}
		si.chunkIter = chnk.NewIterator()
		si.idx = si.idx + 1
	}
	if !si.chunkIter.Scan() {
		si.chunkIter = nil
		goto Start
	}
	sp := si.chunkIter.Value()
	return sp.Timestamp, sp.Value, true
}
