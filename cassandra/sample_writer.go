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

type sampleWriter struct {
	sess *gocql.Session
}

type SampleWriterConfig struct {
	CassandraAddrs []string
	Keyspace       string
	Timeout        time.Duration
}

func NewSampleWriter(config *SampleWriterConfig) (*sampleWriter, error) {
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
	sw := &sampleWriter{
		sess: sess,
	}
	return sw, nil
}

func (sw *sampleWriter) WriteSample(s *bus.Sample) error {
	key, err := convert.MetricToKey(s.Metric)
	if err != nil {
		return err
	}
	return sw.sess.Query(writeSampleCQL, s.Datapoint.Value, key, s.Datapoint.Timestamp).Exec()
}
