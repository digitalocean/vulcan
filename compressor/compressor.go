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

	"github.com/digitalocean/vulcan/model"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/remote"
)

// CompressorConfig needs these dependencies to operate
type CompressorConfig struct {
	Context  context.Context
	Flusher  Flusher
	Consumer consumer.Consumer
}

// Compressor is able to take a stream of metrics and flush them in a compressed format.
// It aims to be resumable.
type Compressor struct {
	cfg *CompressorConfig
}

type chunkLast struct {
	Chunk  chunk.Chunk
	MinTS  int64
	MinOff int64
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

func (c *Compressor) flush(flush map[string][]chunk.Chunk) error {
	for id, chunks := range flush {
		for _, chnk := range chunks {
			err := c.write(id, chnk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Compressor) write(id string, chnk chunk.Chunk) error {
	return nil
	// end, err := chnk.NewIterator().LastTimestamp()
	// if err != nil {
	// 	return err
	// }
	// // TODO is it possible to marshal a resized and fully utilized chunk? As-is, chunks are always 1024 bytes.
	// buf := make([]byte, chunk.ChunkLen)
	// err = chnk.MarshalToBuf(buf)
	// if err != nil {
	// 	return err
	// }
	// sql := `UPDATE compressedtest USING TTL ? SET value = ? WHERE id = ? AND end = ?`
	// c.flushUtilization.Observe(chnk.Utilization())
	// t0 := time.Now()
	// err = c.cfg.Session.Query(sql, c.ttl, buf, id, end).Exec()
	// c.writeDuration.Observe(time.Since(t0).Seconds())
	// return err
}

func (c *Compressor) lastTime(id string) (int64, error) {
	return 0, nil
	// sql := `SELECT end FROM compressedtest WHERE id = ? ORDER BY end DESC LIMIT 1`
	// var end int64
	// t0 := time.Now()
	// err := c.cfg.Session.Query(sql, id).Scan(&end)
	// c.fetchDuration.Observe(time.Since(t0).Seconds())
	// if err == gocql.ErrNotFound {
	// 	return 0, nil
	// }
	// return end, err
}

func parseTimeSeriesBatch(in []byte) (model.TimeSeriesBatch, error) {
	wr := &remote.WriteRequest{}
	if err := proto.Unmarshal(in, wr); err != nil {
		return nil, err
	}
	tsb := make(model.TimeSeriesBatch, 0, len(wr.Timeseries))
	for _, protots := range wr.Timeseries {
		ts := &model.TimeSeries{
			Labels:  map[string]string{},
			Samples: make([]*model.Sample, 0, len(protots.Samples)),
		}
		for _, pair := range protots.Labels {
			ts.Labels[pair.Name] = pair.Value
		}
		for _, protosamp := range protots.Samples {
			ts.Samples = append(ts.Samples, &model.Sample{
				TimestampMS: protosamp.TimestampMs,
				Value:       protosamp.Value,
			})
		}
		tsb = append(tsb, ts)
	}
	return tsb, nil
}
