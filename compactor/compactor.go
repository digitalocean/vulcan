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

package compactor

import (
	"sync"
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/cassandra"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

type compression struct {
	Start time.Time
	Last  time.Time
	Chunk chunk.Chunk
	Ack   func()
}

type Config struct {
	Writer cassandra.Writer
	Source bus.Source
}

type Compactor struct {
	s            bus.Source
	w            cassandra.Writer
	compressions map[string]*compression
}

func (c *Compactor) Run() {
	for p := range c.s.Messages() {
		wg := &sync.WaitGroup{}
		for _, ts := range p.TimeSeriesBatch {
			id := ts.ID()
			if _, ok := c.compressions[id]; !ok {
				wg.Add(1)
				c.compressions[id] = &compression{
					Start: time.Now(), // TODO get last known value from DB
					Last:  time.Now(),
					Chunk: chunk.NewForEncoding(chunk.Varbit),
					Ack: func() {
						wg.Done()
					},
				}
			}
			c.compressions[id].Chunk.Add(model.SamplePair{
				Timestamp: model.Time(ts.Samples[0].TimestampMS)
			})
		}
		go func() {
			wg.Wait()
			p.Ack() // this doesn't work
		}()
	}
}
