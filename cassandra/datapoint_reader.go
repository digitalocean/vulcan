package cassandra

import (
	"time"

	"github.com/gocql/gocql"

	"github.com/digitalocean/vulcan/bus"
)

const fetchUncompressedSQL = `SELECT at, value FROM uncompressed WHERE fqmn = ? AND at >= ? AND at <= ? ORDER BY at DESC`

type datapointReader struct {
	sess *gocql.Session
}

type DatapointReaderConfig struct {
	CassandraAddrs []string
	Keyspace       string
	Timeout        time.Duration
}

func NewDatapointReader(config *DatapointReaderConfig) (*datapointReader, error) {
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
	sw := &datapointReader{
		sess: sess,
	}
	return sw, nil
}

func (dpr *datapointReader) ReadDatapoints(fqmn string, after, before bus.Timestamp) ([]bus.Datapoint, error) {
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
