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
	"context"
	"time"

	"github.com/digitalocean/vulcan/convert"
	"github.com/digitalocean/vulcan/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	cg "github.com/supershabam/sarama-cg"
)

// Config is needed to create a Compressor.
type Config struct {
	Context  context.Context
	Flusher  Flusher
	Laster   Laster
	Consumer cg.Consumer
	MaxAge   time.Duration
	MaxIdle  time.Duration
}

// Compressor is able to take a stream of metrics and flush them in a compressed format.
type Compressor struct {
	cfg     *Config
	maxAge  int64
	maxIdle int64
}

// NewCompressor creates a compressor, but does not start it.
func NewCompressor(cfg *Config) (*Compressor, error) {
	return &Compressor{
		cfg:     cfg,
		maxAge:  cfg.MaxAge.Nanoseconds() / int64(time.Millisecond),
		maxIdle: cfg.MaxIdle.Nanoseconds() / int64(time.Millisecond),
	}, nil
}

// Run executes until completion or error.
func (c *Compressor) Run() error {
	accs := map[string]*acc{}
	for msg := range c.cfg.Consumer.Consume() {
		tsb, err := convert.ParseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		now := c.now(tsb)
		err = c.initMissingAccs(tsb, accs)
		if err != nil {
			return err
		}
		full, err := c.append(tsb, accs, msg.Offset)
		if err != nil {
			return err
		}
		expired := c.expired(now, accs)
		for _, id := range expired {
			full[id] = accs[id].Chunk
			delete(accs, id)
		}
		err = c.cfg.Flusher.Flush(c.cfg.Context, full)
		if err != nil {
			return err
		}
		offset := c.minOffset(accs)
		err = c.cfg.Consumer.CommitOffset(offset)
		if err != nil {
			return err
		}
	}
	return c.cfg.Consumer.Err()
}

func (c Compressor) minOffset(accs map[string]*acc) int64 {
	var offset int64 = -1
	for _, acc := range accs {
		if acc.StartOffset == -1 {
			continue
		}
		if offset == -1 {
			offset = acc.StartOffset
			continue
		}
		if acc.StartOffset < offset {
			offset = acc.StartOffset
		}
	}
	return offset
}

func (c *Compressor) now(tsb model.TimeSeriesBatch) int64 {
	// get first "now" from timestamp in the payload.
	for _, ts := range tsb {
		for _, s := range ts.Samples {
			return s.TimestampMS
		}
	}
	return 0
}

func (c *Compressor) initMissingAccs(tsb model.TimeSeriesBatch, accs map[string]*acc) error {
	missing := []string{}
	for _, ts := range tsb {
		id := ts.ID()
		if _, ok := accs[id]; !ok {
			missing = append(missing, id)
		}
	}
	last, err := c.cfg.Laster.Last(missing)
	if err != nil {
		return err
	}
	for id, t := range last {
		var end int64
		if t != nil {
			end = *t
		}
		a, err := newAcc(end)
		if err != nil {
			return err
		}
		accs[id] = a
	}
	return nil
}

func (c *Compressor) expired(now int64, accs map[string]*acc) []string {
	e := []string{}
	for id, a := range accs {
		if a.old(c.maxAge, now) {
			e = append(e, id)
			continue
		}
		if a.idle(c.maxIdle, now) {
			e = append(e, id)
		}
	}
	return e
}

func (c *Compressor) append(tsb model.TimeSeriesBatch, accs map[string]*acc, offset int64) (map[string]chunk.Chunk, error) {
	full := make(map[string]chunk.Chunk)
	for _, ts := range tsb {
		id := ts.ID()
		a := accs[id]
		for _, s := range ts.Samples {
			overflow, err := a.append(s, offset)
			if err != nil {
				return map[string]chunk.Chunk{}, err
			}
			// assumes that a metric will overflow at max once while processing a time series batch.
			full[id] = overflow
		}
	}
	return full, nil
}
