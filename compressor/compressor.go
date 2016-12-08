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
	pmodel "github.com/prometheus/common/model"
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
// It aims to be resumable.
type Compressor struct {
	cfg     *Config
	maxAge  int64
	maxIdle int64
}

type chunkLast struct {
	Chunk  chunk.Chunk
	MinTS  int64
	MinOff int64
}

type acc struct {
	Chunk chunk.Chunk
	End   int64
}

func NewCompressor(cfg *Config) (*Compressor, error) {
	return &Compressor{
		cfg:     cfg,
		maxAge:  cfg.MaxAge.Nanoseconds() / int64(time.Millisecond),
		maxIdle: cfg.MaxIdle.Nanoseconds() / int64(time.Millisecond),
	}, nil
}

func newAcc(end int64) (*acc, error) {
	c, err := chunk.NewForEncoding(chunk.Varbit)
	if err != nil {
		return nil, err
	}
	return &acc{
		Chunk: c,
		End:   end,
	}, nil
}

func (a *acc) append(s *model.Sample) (chunk.Chunk, error) {
	if s.TimestampMS > a.End {
		return nil, nil
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
	return full, nil
}

func (a *acc) idle(durMS, to int64) bool {
	return to-a.End > durMS
}

func (a *acc) old(durMS, to int64) bool {
	return to-int64(a.Chunk.FirstTime()) > durMS
}

func (c *Compressor) Run() error {
	accs := map[string]*acc{}
	for msg := range c.cfg.Consumer.Consume() {
		tsb, err := convert.ParseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		if len(tsb) == 0 {
			continue
		}
		// get "now" from timestamp in the payload.
		var now int64
	SetNow:
		for _, ts := range tsb {
			for _, s := range ts.Samples {
				now = s.TimestampMS
				break SetNow
			}
		}
		toFlush := map[string]chunk.Chunk{}
		// instantiate new accumulators
		missingIDs := []string{}
		for _, ts := range tsb {
			id := ts.ID()
			if _, ok := accs[id]; !ok {
				missingIDs = append(missingIDs, id)
			}
		}
		last, err := c.cfg.Laster.Last(missingIDs)
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
		// add samples to accumulators
		for _, ts := range tsb {
			id := ts.ID()
			a := accs[id]
			for _, sample := range ts.Samples {
				full, err := a.append(sample)
				if err != nil {
					return err
				}
				if full != nil {
					toFlush[id] = full
				}
			}
		}
		// remove idle and old and mark for flushing
		toRemove := []string{}
		for id, a := range accs {
			if a.old(c.maxAge, now) {
				toRemove = append(toRemove, id)
				toFlush[id] = a.Chunk
				continue
			}
			if a.idle(c.maxIdle, now) {
				toRemove = append(toRemove, id)
				toFlush[id] = a.Chunk
			}
		}
		for _, id := range toRemove {
			delete(accs, id)
		}
		err = c.cfg.Flusher.Flush(c.cfg.Context, toFlush)
		if err != nil {
			return err
		}

	}
	return c.cfg.Consumer.Err()
}

func (c *Compressor) consume(ctx context.Context, topic string, partition int32) error {
	return nil
	// ctx, cancel := context.WithCancel(ctx)
	// defer cancel() // ensure consumer context is canceled nomatter why we exit this function.
	// partitionStr := strconv.FormatInt(int64(partition), 10)
	// defer func() {
	// 	c.highwatermark.DeleteLabelValues(topic, partitionStr)
	// 	c.offset.DeleteLabelValues(topic, partitionStr)
	// 	c.samplesTotal.DeleteLabelValues(topic, partitionStr)
	// }()
	// cr, err := consumer.NewTimeWindow(&consumer.TimeWindowConfig{
	// 	CacheDuration: time.Second,
	// 	Client:        c.cfg.Client,
	// 	Coordinator:   c.cfg.Coordinator,
	// 	Context:       ctx,
	// 	Partition:     partition,
	// 	Start:         consumer.OffsetGroup,
	// 	Topic:         topic,
	// 	Window:        c.cfg.MaxAge * 2,
	// })
	// if err != nil {
	// 	return err
	// }
	// acc := map[string]*chunkLast{}
	// // iterate through messages from this partition in-order committing the offset upon completion.
	// for msg := range cr.Consume() {
	// 	msgt := msg.Timestamp.UnixNano() / int64(time.Millisecond)
	// 	c.highwatermark.WithLabelValues(topic, partitionStr).Set(float64(cr.HighWaterMarkOffset()))
	// 	c.offset.WithLabelValues(topic, partitionStr).Set(float64(msg.Offset))
	// 	flush := map[string][]chunk.Chunk{}
	// 	tsb, err := parseTimeSeriesBatch(msg.Value)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	// handle any new ids including fetching from DB their last timestamp.
	// 	newids := []string{}
	// 	for _, ts := range tsb {
	// 		id := ts.ID()
	// 		if _, ok := acc[id]; !ok {
	// 			newids = append(newids, id)
	// 		}
	// 	}
	// 	wg := &sync.WaitGroup{}
	// 	l := &sync.Mutex{}
	// 	var outerErr error
	// 	wg.Add(len(newids))
	// 	for _, id := range newids {
	// 		go func(id string) {
	// 			defer wg.Done()
	// 			chnk, err := chunk.NewForEncoding(chunk.Varbit)
	// 			if err != nil {
	// 				outerErr = err
	// 				return
	// 			}
	// 			last, err := c.lastTime(id)
	// 			if err != nil {
	// 				outerErr = err
	// 				return
	// 			}
	// 			l.Lock()
	// 			acc[id] = &chunkLast{
	// 				Chunk: chnk,
	// 				MinTS: last,
	// 			}
	// 			l.Unlock()
	// 		}(id)
	// 	}
	// 	wg.Wait()
	// 	if outerErr != nil {
	// 		return err
	// 	}
	// 	for _, ts := range tsb {
	// 		id := ts.ID()
	// 		for _, s := range ts.Samples {
	// 			cl := acc[id]
	// 			// don't process samples that have already been persisted.
	// 			if s.TimestampMS < cl.MinTS {
	// 				continue
	// 			}
	// 			next, err := cl.Chunk.Add(pmodel.SamplePair{
	// 				Timestamp: pmodel.Time(s.TimestampMS),
	// 				Value:     pmodel.SampleValue(s.Value),
	// 			})
	// 			if err != nil {
	// 				return err
	// 			}
	// 			// if added sample to chunk without overflow
	// 			if len(next) == 1 {
	// 				acc[id].Chunk = next[0]
	// 				continue
	// 			}
	// 			if len(next) != 2 {
	// 				panic("appending a sample to chunk should always result in a 1 or 2 element slice")
	// 			}
	// 			// chunk overflowed and we need to flush the full chunk.
	// 			if _, ok := flush[id]; !ok {
	// 				flush[id] = make([]chunk.Chunk, 0, 1)
	// 			}
	// 			flush[id] = append(flush[id], next[0])
	// 			// write overflow samples to new varbit chunk
	// 			iter := next[1].NewIterator()
	// 			chnk, err := chunk.NewForEncoding(chunk.Varbit)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			next[0] = chnk
	// 			for iter.Scan() {
	// 				n, err := next[0].Add(iter.Value())
	// 				if err != nil {
	// 					return err
	// 				}
	// 				if len(n) != 1 {
	// 					panic("expected overflow samples to fit into new varbit chunk")
	// 				}
	// 				next = n
	// 			}
	// 		}
	// 		c.samplesTotal.WithLabelValues(topic, partitionStr).Add(float64(len(ts.Samples)))
	// 	}
	// 	oldids := []string{}
	// 	for id, cl := range acc {
	// 		t := int64(cl.Chunk.FirstTime())
	// 		if msgt-t > c.maxAge {
	// 			if _, ok := flush[id]; !ok {
	// 				flush[id] = make([]chunk.Chunk, 0, 1)
	// 			}
	// 			flush[id] = append(flush[id], cl.Chunk)
	// 			oldids = append(oldids, id)
	// 			continue
	// 		}
	// 		last, err := cl.Chunk.NewIterator().LastTimestamp()
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if msgt-int64(last) > c.maxIdle {
	// 			if _, ok := flush[id]; !ok {
	// 				flush[id] = make([]chunk.Chunk, 0, 1)
	// 			}
	// 			flush[id] = append(flush[id], cl.Chunk)
	// 			oldids = append(oldids, id)
	// 		}
	// 	}
	// 	for _, id := range oldids {
	// 		delete(acc, id)
	// 	}
	// 	// TODO keep to-flush in memory and don't call commit offset each time; instead call commit offset
	// 	// once the in-memory buffer has been flushed (but not on each message read)
	// 	err = c.flush(flush)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = cr.CommitOffset(msg.Offset)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// return cr.Err()
}
