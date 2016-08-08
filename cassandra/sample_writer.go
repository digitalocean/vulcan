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
