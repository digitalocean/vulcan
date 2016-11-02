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
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/model"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

// AccumulatorConfig is used to create a new Accumulator.
type AccumulatorConfig struct {
	Context          context.Context
	Flush            func(buf []byte, start, end int64)
	MaxSampleDelta   time.Duration
	MaxDirtyDuration time.Duration
}

// Accumulator collects samples for a single metric and calls a provided flush
// function when the internal data structure has reached a maximum size limit,
// or the time elapsed between the first and most recent sample exceeds a provided
// maximum, or the time elapsed from making the accumulator dirty to now exceeds
// a provided maximum.
type Accumulator struct {
	c     chunk.Chunk
	cfg   *AccumulatorConfig
	flush *time.Timer
	last  int64
	m     sync.Mutex
}

// NewAccumulator creates an accumulator that will call Flush when it needs to
// based on size and timing constraints.
func NewAccumulator(cfg *AccumulatorConfig) (*Accumulator, error) {
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	a := &Accumulator{
		c:     c,
		cfg:   cfg,
		flush: time.NewTimer(time.Hour * 24 * 365 * 20), // start the timer to trigger 20 years in the future...
	}
	go a.run()
	return a, nil
}

// Append appends a sample to the compressed data structure representing a series of
// samples. If the sample overflows the internal size, then Flush is called and the
// sample is appended to a new internal data structure.
func (a *Accumulator) Append(sample model.Sample) error {
	a.flush.Reset(a.cfg.MaxDirtyDuration)
	if sample.TimestampMS < a.last {
		// silently skip appending samples older than our current position.
		return nil
	}
	a.m.Lock()
	defer a.m.Unlock()
	chunks, err := a.c.Add(pmodel.SamplePair{
		Timestamp: pmodel.Time(sample.TimestampMS),
		Value:     pmodel.SampleValue(sample.Value),
	})
	if err != nil {
		return err
	}
	switch len(chunks) {
	case 1:
		a.c = chunks[0]
	case 2:
		buf := make([]byte, chunk.ChunkLen)
		err := chunks[0].MarshalToBuf(buf)
		if err != nil {
			return err
		}
		start := int64(chunks[0].FirstTime())
		end := a.last
		a.cfg.Flush(buf, start, end)
		a.c, err = chunk.NewForEncoding(chunk.Varbit)
		if err != nil {
			return err
		}
		iter := chunks[1].NewIterator()
		for iter.Scan() {
			chunks, err := a.c.Add(iter.Value())
			if err != nil {
				return err
			}
			if len(chunks) != 1 {
				return fmt.Errorf("expected overflow chunk items to fit into newly allocated chunk")
			}
			a.c = chunks[0]
		}
	default:
		panic("appending a sample to a chunk without error should always result in 1 or 2 elements in chunk slice")
	}
	a.last = sample.TimestampMS
	return nil
}

func (a *Accumulator) run() {
	for {
		select {
		case <-a.cfg.Context.Done():
			a.flush.Stop()
			return
		case <-a.flush.C:
			a.m.Lock()
			buf := make([]byte, chunk.ChunkLen)
			err := a.c.MarshalToBuf(buf)
			if err != nil {
				// TODO this case shouldn't happen, but we should have a better error handling story than logging and continuing.
				logrus.WithError(err).Error("while trying to marshal a chunk to a buffer")
				a.m.Unlock()
				continue
			}
			start := int64(a.c.FirstTime())
			end := a.last
			a.cfg.Flush(buf, start, end)
			a.c, err = chunk.NewForEncoding(chunk.Varbit)
			if err != nil {
				// TODO this case shouldn't happen, but we should have a better error handling story than logging and continuing.
				logrus.WithError(err).Error("while creating a new Varbit chunk")
				a.m.Unlock()
				continue
			}
			a.m.Unlock()
		}
	}
}
