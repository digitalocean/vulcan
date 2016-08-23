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
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/convert"

	"github.com/gocql/gocql"
)

const (
	numCassandraConns    = 2
	cassandraConsistency = gocql.LocalOne
	// we want some pipeline benefits from v4 but this does mean cassandra 2.2 is the min version
	cassandraProtoVersion = 4
)

const (
	writeSampleCQL = `UPDATE uncompressed SET value = ? WHERE fqmn = ? AND at = ?`
)

// SampleWriter represents an object that writes bus messages to the target
// Cassandra database.
type SampleWriter struct {
	sess *gocql.Session
}

// SampleWriterConfig represents the configuration of a SampleWriter.
type SampleWriterConfig struct {
	CassandraAddrs []string
	Keyspace       string
	Timeout        time.Duration
}

// NewSampleWriter creates a new instance of SampleWriter.
func NewSampleWriter(config *SampleWriterConfig) (*SampleWriter, error) {
	cluster := gocql.NewCluster(config.CassandraAddrs...)
	cluster.Keyspace = config.Keyspace
	cluster.Timeout = config.Timeout
	cluster.NumConns = numCassandraConns
	cluster.Consistency = cassandraConsistency
	cluster.ProtoVersion = cassandraProtoVersion
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	sw := &SampleWriter{
		sess: sess,
	}
	return sw, nil
}

// WriteSample implements the storage.WriteSample interface.
func (sw *SampleWriter) WriteSample(s *bus.Sample) error {
	key, err := convert.MetricToKey(s.Metric)
	if err != nil {
		return err
	}
	return sw.sess.Query(writeSampleCQL, s.Datapoint.Value, key, s.Datapoint.Timestamp).Exec()
}
