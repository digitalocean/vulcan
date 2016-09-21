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
	"github.com/gocql/gocql"
)

// Writer implements the ingester.Writer interface for persisting
// TimeSeriesBatch to cassandra.
type Writer struct {
}

func (w *Writer) Write(tsb bus.TimeSeriesBatch) error {

}

// NewWriter creates a new instance of Writer.
func NewWriter(config *WriterConfig) (*Writer, error) {
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
	sw := &Writer{
		sess: sess,
	}
	return sw, nil
}
