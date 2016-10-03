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

	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"

	"github.com/digitalocean/vulcan/bus"
)

const fetchUncompressedSQL = `SELECT at, value FROM uncompressed WHERE fqmn = ? AND at >= ? AND at <= ? ORDER BY at DESC`

// DatapointReader represents an object that queries on the target Cassandra
// server and transforms the result to a Vulcan Datapoint.
type DatapointReader struct {
	sess *gocql.Session
}

// DatapointReaderConfig represents the configuration of a DatapointReader.
type DatapointReaderConfig struct {
	CassandraAddrs []string
	Keyspace       string
	Timeout        time.Duration
}

// NewDatapointReader creates a new instance of DatapointReader.
func NewDatapointReader(config *DatapointReaderConfig) (*DatapointReader, error) {
	cluster := gocql.NewCluster(config.CassandraAddrs...)
	cluster.Keyspace = config.Keyspace
	cluster.Timeout = config.Timeout
	cluster.NumConns = numCassandraConns
	cluster.Consistency = cassandraConsistency
	cluster.ProtoVersion = cassandraProtoVersion
	// Fallback simple host pool distributes queries and prevents sending queries to unresponsive hosts.
	fallbackHostPolicy := gocql.HostPoolHostPolicy(hostpool.New(nil))
	// Token-aware policy performs queries against a host responsible for the partition.
	// TODO in gocql make token-aware able to write to any host for a partition when the
	// replication factor is > 1.
	// https://github.com/gocql/gocql/blob/4f49cd01c8939ce7624952fe286c3d08c4be7fa1/policies.go#L331
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallbackHostPolicy)
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	sw := &DatapointReader{
		sess: sess,
	}
	return sw, nil
}

// ReadDatapoints implements storage.DatapointReader interface.
func (dpr *DatapointReader) ReadDatapoints(fqmn string, after, before bus.Timestamp) ([]bus.Datapoint, error) {
	query := dpr.sess.Query(fetchUncompressedSQL, fqmn, after, before)
	iter := query.Iter()
	var (
		at    int64
		value float64
	)
	datapoints := []bus.Datapoint{}
	for iter.Scan(&at, &value) {
		datapoints = append(datapoints, bus.Datapoint{
			Timestamp: bus.Timestamp(at),
			Value:     value,
		})
	}
	err := iter.Close()
	return datapoints, err
}
