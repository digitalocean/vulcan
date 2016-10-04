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
	"github.com/gocql/gocql"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

// IteratorFactory is able to create SeriesIterator that talk to a IteratorFactory.
// This bridges the new series iterator prometheus interface to our older cassandra
// code that was built for a different interface. Eventually, a cassandra storage
// object should just be able to create iterators directly.
type IteratorFactory struct {
	Session  *gocql.Session
	PageSize int
	Prefetch float64
}

// Iterator returns a new SeriesIterator.
func (itrf *IteratorFactory) Iterator(m metric.Metric, from, through model.Time) (local.SeriesIterator, error) {
	return NewSeriesIterator(&SeriesIteratorConfig{
		Session:  itrf.Session,
		Metric:   m,
		After:    from,
		Before:   through,
		PageSize: itrf.PageSize,
		Prefetch: itrf.Prefetch,
	}), nil
}
