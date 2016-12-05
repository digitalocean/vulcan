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
	"time"

	"github.com/digitalocean/vulcan/model"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
)

type stmt string

const (
	getLastSample stmt = `lastSample`
)

var stmts = map[stmt]string{
	getLastSample: `SELECT at, value FROM %s WHERE fqmn= ? ORDER BY AT DESC LIMIT 1`,
}

// Reader represents the behavior of an object that reads records from Cassandra.
type Reader interface {
	GetLastSample(fqmn string) (*model.Sample, error)
}

var (
	_ Reader               = &Read{}
	_ prometheus.Collector = &Read{}
)

// Read reads records from cassandra.
type Read struct {
	session *gocql.Session
	stmts   map[stmt]string

	readDuration prometheus.Histogram
}

// ReaderConfig represents the configuration parameters of a Reader instance.
type ReaderConfig struct {
	NumWorkers int
	Session    *gocql.Session
	TableName  string
	Keyspace   string
}

// NewReader returns a new instance of Reader.
func NewReader(config *ReaderConfig) Reader {
	var preparedStmts = make(map[stmt]string, len(stmts))

	for k, v := range stmts {
		preparedStmts[k] = fmt.Sprintf(
			v,
			fmt.Sprintf("%s.%s", config.Keyspace, config.TableName),
		)
	}

	return &Read{
		session: config.Session,
		stmts:   preparedStmts,

		readDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "read_duration_seconds",
				Help:      "Histogram of seconds elapsed to reading from cassandra.",
				Buckets:   prometheus.DefBuckets,
			},
		),
	}
}

// Describe implements prometheus.Collector.
func (r *Read) Describe(ch chan<- *prometheus.Desc) {
	r.readDuration.Describe(ch)
}

// Collect implements prometheus.Collector.
func (r *Read) Collect(ch chan<- prometheus.Metric) {
	r.readDuration.Collect(ch)
}

// GetLastSample returns the latest recorded sample of the metric.
// If no metric is found, returned sample will have nil values for its
// attributes.  The assumption is that the timestamp should never be 0.
func (r *Read) GetLastSample(fqmn string) (*model.Sample, error) {
	var (
		s  = &model.Sample{}
		t0 = time.Now()
	)
	defer func() { r.readDuration.Observe(time.Since(t0).Seconds()) }()

	err := r.session.Query(r.stmts[getLastSample], fqmn).Scan(&s.TimestampMS, &s.Value)
	if err != nil && err != gocql.ErrNotFound {
		return nil, err
	}

	return s, nil
}
