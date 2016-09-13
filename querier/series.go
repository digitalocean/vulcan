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

package querier

import (
	"log"
	"sync"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"
	"github.com/digitalocean/vulcan/storage"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

// DatapointReader is able to create SeriesIterator that talk to a DatapointReader.
// This bridges the new series iterator prometheus interface to our older cassandra
// code that was built for a different interface. Eventually, a cassandra storage
// object should just be able to create iterators directly.
type DatapointReader struct {
	DR storage.DatapointReader
}

// Iterator returns a new SeriesIterator
func (dr *DatapointReader) Iterator(m metric.Metric, from, through model.Time) (local.SeriesIterator, error) {
	key, err := convert.MetricToKey(toBusMetric(m.Metric))
	if err != nil {
		return nil, err
	}
	si := &SeriesIterator{
		After:    from,
		Before:   through,
		Key:      key,
		M:        m,
		DPReader: dr.DR,

		points: []bus.Datapoint{},
	}

	return si, nil
}

// SeriesIterator enables efficient access of sample values in a series. Its
// methods are not goroutine-safe. A SeriesIterator iterates over a snapshot of
// a series, i.e. it is safe to continue using a SeriesIterator after or during
// modifying the corresponding series, but the iterator will represent the state
// of the series prior to the modification.
type SeriesIterator struct {
	After, Before model.Time
	M             metric.Metric
	Key           string
	DPReader      storage.DatapointReader

	l      sync.Mutex
	ready  bool
	points []bus.Datapoint
}

// ValueAtOrBeforeTime gets the value that is closest before the given time. In case a value
// exists at precisely the given time, that value is returned. If no
// applicable value exists, ZeroSamplePair is returned.
func (si *SeriesIterator) ValueAtOrBeforeTime(t model.Time) model.SamplePair {
	si.fetch()
	for _, point := range si.points {
		if int64(point.Timestamp) <= int64(t) {
			return model.SamplePair{
				Timestamp: model.Time(point.Timestamp),
				Value:     model.SampleValue(point.Value),
			}
		}
	}
	return local.ZeroSamplePair
}

// RangeValues gets all values contained within a given interval.
func (si *SeriesIterator) RangeValues(r metric.Interval) []model.SamplePair {
	si.fetch()
	result := []model.SamplePair{}
	for i := len(si.points) - 1; i > 0; i-- {
		point := si.points[i]
		if int64(r.OldestInclusive) <= int64(point.Timestamp) && int64(point.Timestamp) <= int64(r.NewestInclusive) {
			result = append(result, model.SamplePair{
				Timestamp: model.Time(point.Timestamp),
				Value:     model.SampleValue(point.Value),
			})
		}
	}
	return result
}

// Metric returns the metric of the series that the iterator corresponds to.
func (si *SeriesIterator) Metric() metric.Metric {
	return si.M
}

// Close closes the iterator and releases the underlying data.
func (si *SeriesIterator) Close() {
	return
}

func (si *SeriesIterator) fetch() {
	// early unsynchronized exit
	if si.ready {
		return
	}
	si.l.Lock()
	defer si.l.Unlock()
	// recheck condition
	if si.ready {
		return
	}
	points, err := si.DPReader.ReadDatapoints(si.Key, bus.Timestamp(si.After), bus.Timestamp(si.Before))
	if err != nil {
		log.Println(err)
	}
	si.points = points
	si.ready = true
}
