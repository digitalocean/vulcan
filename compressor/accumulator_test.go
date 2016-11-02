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

package compressor_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/compressor"
	"github.com/digitalocean/vulcan/model"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
)

func makeFullChunk(start time.Time) (c chunk.Chunk, end time.Time, err error) {
	var value float64
	c, err = chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return
	}
	chunks := []chunk.Chunk{c}
	for len(chunks) < 2 {
		// increment time by 15s + [0,1]μs for some variation
		start = start.Add(time.Second*15 + time.Duration(rand.Int63n(int64(time.Microsecond))))
		innerChunks, err := chunks[0].Add(pmodel.SamplePair{
			Timestamp: pmodel.Time(start.UnixNano() / int64(time.Millisecond)),
			Value:     pmodel.SampleValue(value),
		})
		if err != nil {
			return c, end, err
		}
		chunks = innerChunks
	}
	return chunks[0], start, nil
}

func chunkEquals(c1, c2 chunk.Chunk) bool {
	i1 := c1.NewIterator()
	i2 := c2.NewIterator()
	for i1.Scan() {
		ok := i2.Scan()
		if !ok {
			return false
		}
		v1 := i1.Value()
		v2 := i2.Value()
		if !v1.Equal(&v2) {
			return false
		}
	}
	// if i2 can still scan, not equal
	return !i2.Scan()
}

func TestAccumulator(t *testing.T) {
	const longForm = "Jan 2, 2006 at 3:04pm (MST)"
	start, err := time.Parse(longForm, "Feb 3, 2013 at 7:54pm (PST)")
	if err != nil {
		t.Fatal(err)
	}
	full1, end1, err := makeFullChunk(start)
	if err != nil {
		t.Fatal(err)
	}
	full2, _, err := makeFullChunk(end1)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("acc", func(t *testing.T) {
		t.Run("equals", func(t *testing.T) {
			t.Parallel()
			if !chunkEquals(full1, full1) {
				t.Fatalf("expected full to eq full")
			}
		})
		t.Run("size", func(t *testing.T) {
			t.Parallel()
			// this tests that the accumulator calls flush after a max internal size is reached.
			full1 := full1.Clone()
			full2 := full2.Clone()
			ctx := context.Background()
			var counter int64
			var lastTime int64
			flush := func(buf []byte, start, end int64) {
				counter++
				c, err := chunk.NewForEncoding(chunk.Varbit)
				if err != nil {
					t.Fatal(err)
				}
				err = c.UnmarshalFromBuf(buf)
				if err != nil {
					t.Fatal(err)
				}
				if end != lastTime {
					t.Errorf("expected flush end time to be %d but got %d", lastTime, end)
				}
				if counter == 1 {
					if !chunkEquals(full1, c) {
						t.Errorf("expected flushed chunk to equal full chunk")
					}
					if start != int64(full1.FirstTime()) {
						t.Errorf("expected flush start time to be %d but got %d", full1.FirstTime(), start)
					}
					return
				}
				if !chunkEquals(full2, c) {
					t.Errorf("expected flushed chunk to equal full2 chunk")
				}
				if start != int64(full2.FirstTime()) {
					t.Errorf("expected flush start time to be %d but got %d", full2.FirstTime(), start)
				}
			}
			a, err := compressor.NewAccumulator(&compressor.AccumulatorConfig{
				Context:          ctx,
				Flush:            flush,
				MaxDirtyDuration: time.Hour * 24 * 365,
				MaxSampleDelta:   time.Hour * 24 * 365,
			})
			if err != nil {
				t.Fatal(err)
			}
			iter := full1.NewIterator()
			for iter.Scan() {
				v := iter.Value()
				s := model.Sample{
					TimestampMS: int64(v.Timestamp),
					Value:       float64(v.Value),
				}
				err = a.Append(s)
				if err != nil {
					t.Fatal(err)
				}
				lastTime = int64(v.Timestamp)
				// counter should stay at 0 the entire time
				if counter != 0 {
					t.Fatalf("expected counter to be %d but got %d", 0, counter)
				}
			}
			iter = full2.NewIterator()
			for iter.Scan() {
				v := iter.Value()
				s := model.Sample{
					TimestampMS: int64(v.Timestamp),
					Value:       float64(v.Value),
				}
				err = a.Append(s)
				if err != nil {
					t.Fatal(err)
				}
				lastTime = int64(v.Timestamp)
				// counter should stay at 1 the entire time.
				if counter != 1 {
					t.Errorf("expected counter to be %d but got %d", 1, counter)
				}
			}
			// now our accumulator is full again, one more sample will overflow it and cause it to flush.
			err = a.Append(model.Sample{
				TimestampMS: lastTime + 15000,
				Value:       42.0,
			})
			if err != nil {
				t.Fatal(err)
			}
			if counter != 2 {
				t.Fatalf("expected counter to be %d but got %d", 1, counter)
			}
		})
		t.Run("dirty", func(t *testing.T) {
			// this tests that flush is called after an elapsed time after writing a sample.
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var counter int64
			done1 := make(chan struct{})
			done2 := make(chan struct{})
			flush := func(buf []byte, start, end int64) {
				counter++
				if counter == 1 {
					close(done1)
					if start != 24 {
						t.Errorf("expected start to be %d but got %d", 24, start)
					}
				}
				if counter == 2 {
					close(done2)
					if start != 42 {
						t.Errorf("expected start to be %d but got %d", 42, start)
					}
				}
			}
			a, err := compressor.NewAccumulator(&compressor.AccumulatorConfig{
				Context:          ctx,
				Flush:            flush,
				MaxDirtyDuration: time.Millisecond,
				MaxSampleDelta:   time.Hour * 24 * 365,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = a.Append(model.Sample{
				TimestampMS: 24,
				Value:       42.0,
			})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-done1:
			case <-time.After(time.Millisecond * 20):
				t.Fatalf("expected flush to be called")
			}
			// we don't want the accumulator to be constantly flushing
			time.Sleep(time.Millisecond * 20)
			if counter != 1 {
				t.Fatalf("expected counter to be %d but was %d", 1, counter)
			}
			err = a.Append(model.Sample{
				TimestampMS: 42,
				Value:       42.0,
			})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-done2:
			case <-time.After(time.Millisecond * 20):
				t.Fatalf("expected flush to be called")
			}
		})
	})
}
