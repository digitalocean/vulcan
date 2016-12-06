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

package compressor

import "github.com/prometheus/prometheus/storage/local/chunk"

// Overwriter attemps to pack the most samples per byte into Cassandra. Because
// we have to flush varbit encoded data to cassanddra based on time, we are often
// flushing 1k varbit blocks that have space left for more samples. Using the overwriter,
// we read the last varbit block in the database to see if we can further compress datapoints
// into that block. This causes tombstones in cassandra, but should be compacted away after
// an early few compaction cycles. This seems worth it to get better compaction. In an attempt
// to make sure we don't cause compaction in Cassandra for data that is too old and already
// compacted into large sstables we no longer want to alter, we can set a time threshold that
// won't attempt to recompact blocks older than that threshold.
type Overwriter struct {
}

// Flush does the thing.
func (o *Overwriter) Flush(string id, c chunk.Chunk) error {
	/*
	   table:
	       id: string
	       start: bigint (int64 timestamp in ms)
	       chunk: blob
	   primary key (id, start)
	   clustering order by (start)
	*/
	// get last chunk from database
	// for chunk, overflow := append(next) {
	//   if overflow {
	//     upsert id,start,chunk
	//     chunk = overflow
	//   }
	// }
	// new id,start,chunk
}
