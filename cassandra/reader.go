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

import "github.com/gocql/gocql"

const (
	lastTimestampQuery = `SELECT end FROM compressed WHERE fqmn = ? ORDER BY start DESC LIMIT 1`
)

// ReaderConfig is used to create a new reader.
type ReaderConfig struct {
	Sess *gocql.Session
}

// Reader provides a read path from cassandra.
type Reader struct {
	sess *gocql.Session
}

// NewReader returns a new Reader.
func NewReader(cfg *ReaderConfig) (*Reader, error) {
	return &Reader{
		sess: cfg.Sess,
	}, nil
}

// LastTimestampMS returns if the id exists or not and the last known timestamp
// for that id if it exists.
func (r *Reader) LastTimestampMS(id string) (bool, int64, error) {
	iter := r.sess.Query(lastTimestampQuery, id).Iter()
	var end int64
	ok := iter.Scan(&end)
	err := iter.Close()
	if err != nil {
		return false, 0, err
	}
	if !ok {
		return false, 0, nil
	}
	return true, end, nil
}
