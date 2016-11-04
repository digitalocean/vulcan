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

package crazy

import (
	"sync"
	"time"

	"github.com/digitalocean/vulcan/model"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

// AccumulatorConfig is used to create a new Accumulator.
type AccumulatorConfig struct {
	MaxAge time.Duration
}

type chunkend struct {
	Chunk chunk.Chunk
	End   int64
}

// Accumulator collects samples for a single metric and calls a provided flush
// function when the internal data structure has reached a maximum size limit,
// or the time elapsed between the first and most recent sample exceeds a provided
// maximum, or the time elapsed from making the accumulator dirty to now exceeds
// a provided maximum.
type Accumulator struct {
	chunks []*chunkend
	m      sync.Mutex
	maxAge int64
}

// NewAccumulator creates an accumulator that will call Flush when it needs to
// based on size and timing constraints.
func NewAccumulator(cfg *AccumulatorConfig) (*Accumulator, error) {
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	a := &Accumulator{
		chunks: []*chunkend{
			&chunkend{
				Chunk: c,
				End:   0,
			},
		},
		maxAge: cfg.MaxAge.Nanoseconds() / int64(time.Millisecond),
	}
	return a, nil
}

// Append appends a sample to the accumulator. If this causes
func (a *Accumulator) Append(sample *model.Sample) error {
	a.m.Lock()
	defer a.m.Unlock()
	if sample.TimestampMS < a.chunks[0].End {
		return nil
	}
	chunks, err := a.chunks[0].Chunk.Add(pmodel.SamplePair{
		Timestamp: pmodel.Time(sample.TimestampMS),
		Value:     pmodel.SampleValue(sample.Value),
	})
	if err != nil {
		return err
	}
	switch len(chunks) {
	case 1:
		a.chunks[0].Chunk = chunks[0]
		a.chunks[0].End = sample.TimestampMS
	case 2:
		// the end value of the just-overflowed-chunk is still valid.
		a.chunks[0].Chunk = chunks[0]
		next, err := chunk.NewForEncoding(chunk.Varbit)
		if err != nil {
			return err
		}
		// write overflow samples to new chunk
		iter := chunks[1].NewIterator()
		for iter.Scan() {
			chunks, err := next.Add(iter.Value())
			if err != nil {
				return err
			}
			if len(chunks) != 1 {
				panic("expected overflow samples to fit into new varbit chunk")
			}
			next = chunks[0]
		}
		// prepend
		a.chunks = append([]*chunkend{
			{
				Chunk: next,
				End:   sample.TimestampMS,
			},
		}, a.chunks...)
	default:
		panic("appending a sample to a chunk without error should always result in 1 or 2 elements in chunk slice")
	}
	// remove expired chunks
	cutoff := sample.TimestampMS - a.maxAge
	for len(a.chunks) > 0 {
		end := a.chunks[len(a.chunks)-1].End
		if end == 0 || end >= cutoff {
			break
		}
		a.chunks = a.chunks[:len(a.chunks)-1]
	}
	return nil
}

// ChunksAfter returns a clone of all chunks in-time-ascending-order which contain
// at least one value after the provided time. The result may be a length zero slice.
func (a *Accumulator) ChunksAfter(after int64) []chunk.Chunk {
	chunks := make([]chunk.Chunk, 0)
	a.m.Lock()
	defer a.m.Unlock()
	for i := len(a.chunks) - 1; i >= 0; i-- {
		if a.chunks[i].End > after {
			chunks = append(chunks, a.chunks[i].Chunk.Clone())
		}
	}
	return chunks
}

// Last returns the last appended timestamp in ms.
func (a *Accumulator) Last() int64 {
	return a.chunks[0].End
}
