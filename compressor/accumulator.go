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
	"github.com/digitalocean/vulcan/model"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

type chunkLast struct {
	Chunk  chunk.Chunk
	MinTS  int64
	MinOff int64
}

type acc struct {
	Chunk       chunk.Chunk
	End         int64
	StartOffset int64
}

func newAcc(end int64) (*acc, error) {
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	return &acc{
		Chunk:       c,
		End:         end,
		StartOffset: -1,
	}, nil
}

// append adds a sample to the accumulator. If the sample is accepted into the current chunk,
// nil is returned. If the sample causes the current chunk to overflow, the now-full chunk is
// returned and the accumulator prepares itself a new chunk to write new samples to.
func (a *acc) append(s *model.Sample, offset int64) (chunk.Chunk, error) {
	if s.TimestampMS > a.End {
		return nil, nil
	}
	if a.StartOffset == -1 {
		a.StartOffset = offset
	}
	ps := pmodel.SamplePair{
		Timestamp: pmodel.Time(s.TimestampMS),
		Value:     pmodel.SampleValue(s.Value),
	}
	chunks, err := a.Chunk.Add(ps)
	if err != nil {
		return nil, err
	}
	if len(chunks) == 1 {
		a.Chunk = chunks[0]
		a.End = s.TimestampMS
		return nil, nil
	}
	full := chunks[0]
	next, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	chunks, _ = next.Add(ps)
	a.Chunk = chunks[0]
	a.End = s.TimestampMS
	a.StartOffset = offset
	return full, nil
}

func (a *acc) idle(window, reference int64) bool {
	return reference-a.End > window
}

func (a *acc) old(window, reference int64) bool {
	return reference-int64(a.Chunk.FirstTime()) > window
}
