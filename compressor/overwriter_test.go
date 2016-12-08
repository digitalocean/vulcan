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

import (
	"math/rand"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/compressor"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

var _ compressor.Flusher = &compressor.Overwriter{}

func makeUtilChunk(start time.Time, targetUtil float64) (chunk.Chunk, time.Time, error) {
	var value float64
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, start, err
	}
	cur := start
	for c.Utilization() < targetUtil {
		// increment time by 15s + [0,1]μs for some variation
		cur = cur.Add(time.Second*15 + time.Duration(rand.Int63n(int64(time.Microsecond))))
		chunks, err := c.Add(pmodel.SamplePair{
			Timestamp: pmodel.Time(cur.UnixNano() / int64(time.Millisecond)),
			Value:     pmodel.SampleValue(value),
		})
		if err != nil {
			return nil, cur, err
		}
		c = chunks[0]
	}
	return c, cur, nil
}

func TestCompact(t *testing.T) {
	chunks := make([]chunk.Chunk, 0)
	c, end, err := makeUtilChunk(time.Now(), 0.50)
	if err != nil {
		t.Fatal(err)
	}
	chunks = append(chunks, c)
	c, _, err = makeUtilChunk(end, 0.10)
	if err != nil {
		t.Fatal(err)
	}
	chunks = append(chunks, c)
	chunks, err = compact(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Errorf("expected chunks to be compacted into 1 chunk not %d", len(chunks))
	}
}
