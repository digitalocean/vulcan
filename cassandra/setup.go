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

	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
)

// SetupTables sets up the provided keyspace and creates table `uncompressed`
// on the provided Cassandra hosts.
func SetupTables(hosts []string, keyspace string) error {
	first := gocql.NewCluster(hosts...)
	firstSess, err := first.CreateSession()
	if err != nil {
		return err
	}
	flog := log.WithFields(log.Fields{
		"keyspace": keyspace,
		"hosts":    hosts,
	})
	flog.Debug("ensuring keyspace")
	keyspaceCQL := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
AND durable_writes = true;`, keyspace)
	tableCQL := `CREATE TABLE IF NOT EXISTS uncompressed
(
  fqmn  text,
  at    bigint,
  value double,
  PRIMARY KEY (fqmn, at)
) WITH COMPACT STORAGE
  AND CLUSTERING ORDER BY (at ASC)
  AND compaction = {'class': 'DateTieredCompactionStrategy', 'min_threshold': '12', 'max_threshold': '32', 'max_sstable_age_days': '0.083', 'base_time_seconds': '50' }
  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};`
	err = firstSess.Query(keyspaceCQL).Exec()
	if err != nil {
		return err
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	sess, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	flog.Debug("ensuring table")
	err = sess.Query(tableCQL).Exec()
	if err != nil {
		return err
	}
	return nil
}
