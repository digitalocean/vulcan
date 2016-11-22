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
	"math"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/model"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/remote"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/consumer"
)

type CompressorConfig struct {
	Client      sarama.Client
	Coordinator *cg.Coordinator
	GroupID     string
	MaxAge      time.Duration
	MaxIdle     time.Duration
	Session     *gocql.Session
	TTL         time.Duration
}

type Compressor struct {
	cfg              *CompressorConfig
	maxAge           int64 // max age in milliseconds
	maxIdle          int64 // max idle in milliseconds
	ttl              int64 // time to live in seconds.
	fetchDuration    prometheus.Summary
	flushUtilization prometheus.Summary
	highwatermark    *prometheus.GaugeVec
	offset           *prometheus.GaugeVec
	samplesTotal     *prometheus.CounterVec
	writeDuration    prometheus.Summary
}

func NewCompressor(cfg *CompressorConfig) (*Compressor, error) {
	return &Compressor{
		cfg:     cfg,
		maxAge:  int64(cfg.MaxAge.Seconds()),
		maxIdle: int64(cfg.MaxIdle.Seconds()),
		ttl:     int64(cfg.TTL.Seconds()),
		fetchDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: "vulcan",
			Subsystem: "compressor",
			Name:      "fetch_duration_seconds",
			Help:      "Summary of durations taken getting end time from database",
		}),
		flushUtilization: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: "vulcan",
			Subsystem: "compressor",
			Name:      "flush_utilization",
			Help:      "Summary of utilization percentage 0-1 of a flushed chunk",
			Objectives: map[float64]float64{
				0.1: 0.01,
				0.5: 0.05,
				0.9: 0.01,
			},
		}),
		highwatermark: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "vulcan",
			Subsystem: "compressor",
			Name:      "highwatermark",
			Help:      "Current highwatermark of a topic-partition",
		}, []string{"topic", "partition"}),
		offset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "vulcan",
			Subsystem: "compressor",
			Name:      "offset",
			Help:      "Current offset of a topic-partition",
		}, []string{"topic", "partition"}),
		samplesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vulcan",
			Subsystem: "compressor",
			Name:      "samples_total",
			Help:      "count of samples ingested into compressor",
		}, []string{"topic", "partition"}),
		writeDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: "vulcan",
			Subsystem: "compressor",
			Name:      "write_duration_seconds",
			Help:      "Summary of durations taken writing a chunk to the database",
		}),
	}, nil
}

// Describe implements prometheus.Collector.
func (c *Compressor) Describe(ch chan<- *prometheus.Desc) {
	c.fetchDuration.Describe(ch)
	c.flushUtilization.Describe(ch)
	c.highwatermark.Describe(ch)
	c.offset.Describe(ch)
	c.samplesTotal.Describe(ch)
	c.writeDuration.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *Compressor) Collect(ch chan<- prometheus.Metric) {
	c.fetchDuration.Collect(ch)
	c.flushUtilization.Collect(ch)
	c.highwatermark.Collect(ch)
	c.offset.Collect(ch)
	c.samplesTotal.Collect(ch)
	c.writeDuration.Collect(ch)
}

func (c *Compressor) Run() error {
	return c.cfg.Coordinator.Run(c.handle)
}

func (c *Compressor) handle(ctx context.Context, topic string, partition int32) {
	if partition != 2 {
		return
	}
	log := logrus.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	})
	log.Info("taking control of topic-partition")
	count := 0
	backoff := time.NewTimer(time.Duration(0))
	for {
		count++
		select {
		case <-ctx.Done():
			return
		case <-backoff.C:
			err := c.consume(ctx, topic, partition)
			if err == nil {
				logrus.WithFields(logrus.Fields{
					"topic":     topic,
					"partition": partition,
				}).Info("relenquishing control of topic-partition")
				return
			}
			// exponential backoff with cap at 10m
			dur := time.Duration(math.Min(float64(time.Minute*10), float64(100*time.Millisecond)*math.Pow(float64(2), float64(count))))
			logrus.WithFields(logrus.Fields{
				"backoff_duration": dur,
				"topic":            topic,
				"partition":        partition,
			}).WithError(err).Error("error while consuming but restarting after backoff")
			backoff.Reset(dur)
		}
	}
}

type chunkLast struct {
	Chunk chunk.Chunk
	MinTS int64
}

func (c *Compressor) consume(ctx context.Context, topic string, partition int32) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // ensure consumer context is canceled nomatter why we exit this function.
	partitionStr := strconv.FormatInt(int64(partition), 10)
	defer func() {
		c.highwatermark.DeleteLabelValues(topic, partitionStr)
		c.offset.DeleteLabelValues(topic, partitionStr)
		c.samplesTotal.DeleteLabelValues(topic, partitionStr)
	}()
	cr, err := consumer.NewSeek(&consumer.SeekConfig{
		CacheDuration: time.Second,
		Client:        c.cfg.Client,
		Coordinator:   c.cfg.Coordinator,
		Context:       ctx,
		Partition:     partition,
		SeekFn:        c.seek,
		Topic:         topic,
	})
	if err != nil {
		return err
	}
	acc := map[string]*chunkLast{}
	// iterate through messages from this partition in-order committing the offset upon completion.
	for msg := range cr.Consume() {
		msgt := msg.Timestamp.UnixNano() / int64(time.Millisecond)
		c.highwatermark.WithLabelValues(topic, partitionStr).Set(float64(cr.HighWaterMarkOffset()))
		c.offset.WithLabelValues(topic, partitionStr).Set(float64(msg.Offset))
		flush := map[string][]chunk.Chunk{}
		tsb, err := parseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		for _, ts := range tsb {
			id := ts.ID()
			if _, ok := acc[id]; !ok {
				chnk, err := chunk.NewForEncoding(chunk.Varbit)
				if err != nil {
					return err
				}
				// TODO batch all missing IDs and fetch their times concurrently.
				last, err := c.lastTime(id)
				if err != nil {
					return err
				}
				acc[id] = &chunkLast{
					Chunk: chnk,
					MinTS: last,
				}
			}
			for _, s := range ts.Samples {
				cl := acc[id]
				// don't process samples that have already been persisted.
				if s.TimestampMS < cl.MinTS {
					continue
				}
				next, err := cl.Chunk.Add(pmodel.SamplePair{
					Timestamp: pmodel.Time(s.TimestampMS),
					Value:     pmodel.SampleValue(s.Value),
				})
				if err != nil {
					return err
				}
				// if added sample to chunk without overflow
				if len(next) == 1 {
					acc[id].Chunk = next[0]
					continue
				}
				if len(next) != 2 {
					panic("appending a sample to chunk should always result in a 1 or 2 element slice")
				}
				// chunk overflowed and we need to flush the full chunk.
				if _, ok := flush[id]; !ok {
					flush[id] = make([]chunk.Chunk, 0, 1)
				}
				flush[id] = append(flush[id], next[0])
				// write overflow samples to new varbit chunk
				iter := next[1].NewIterator()
				chnk, err := chunk.NewForEncoding(chunk.Varbit)
				if err != nil {
					return err
				}
				next[0] = chnk
				for iter.Scan() {
					n, err := next[0].Add(iter.Value())
					if err != nil {
						return err
					}
					if len(n) != 1 {
						panic("expected overflow samples to fit into new varbit chunk")
					}
					next = n
				}
			}
			c.samplesTotal.WithLabelValues(topic, partitionStr).Add(float64(len(ts.Samples)))
		}
		oldids := []string{}
		for id, cl := range acc {
			t := int64(cl.Chunk.FirstTime())
			if msgt-t > c.maxAge {
				if _, ok := flush[id]; !ok {
					flush[id] = make([]chunk.Chunk, 0, 1)
				}
				flush[id] = append(flush[id], cl.Chunk)
				oldids = append(oldids, id)
				continue
			}
			last, err := cl.Chunk.NewIterator().LastTimestamp()
			if err != nil {
				return err
			}
			if msgt-int64(last) > c.maxIdle {
				if _, ok := flush[id]; !ok {
					flush[id] = make([]chunk.Chunk, 0, 1)
				}
				flush[id] = append(flush[id], cl.Chunk)
				oldids = append(oldids, id)
			}
		}
		for _, id := range oldids {
			delete(acc, id)
		}
		// TODO keep to-flush in memory and don't call commit offset each time; instead call commit offset
		// once the in-memory buffer has been flushed (but not on each message read)
		err = c.flush(flush)
		if err != nil {
			return err
		}
		err = cr.CommitOffset(msg.Offset)
		if err != nil {
			return err
		}
	}
	return cr.Err()
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

func (c *Compressor) seek(topic string, partition int32) (int64, error) {
	b, err := c.cfg.Client.Coordinator(c.cfg.GroupID)
	if err != nil {
		return 0, err
	}
	req := &sarama.OffsetFetchRequest{
		ConsumerGroup: c.cfg.GroupID,
		Version:       1,
	}
	req.AddPartition(topic, partition)
	resp, err := b.FetchOffset(req)
	if err != nil {
		return 0, err
	}
	blk := resp.GetBlock(topic, partition)
	if blk.Err != sarama.ErrNoError {
		return 0, blk.Err
	}
	logrus.WithField("offset", blk.Offset).Info("seek to offset")
	if blk.Offset < 0 {
		return c.cfg.Client.GetOffset(topic, partition, blk.Offset)
	}
	return blk.Offset, nil
}

func (c *Compressor) write(id string, chnk chunk.Chunk) error {
	end, err := chnk.NewIterator().LastTimestamp()
	if err != nil {
		return err
	}
	// TODO is it possible to marshal a resized and fully utilized chunk? As-is, chunks are always 1024 bytes.
	var buf []byte
	err = chnk.MarshalToBuf(buf)
	if err != nil {
		return err
	}
	sql := `UPDATE compressed USING TTL ? SET value = ? WHERE id = ? AND end = ?`
	c.flushUtilization.Observe(chnk.Utilization())
	t0 := time.Now()
	err = c.cfg.Session.Query(sql, c.ttl, buf, id, end).Exec()
	c.writeDuration.Observe(time.Since(t0).Seconds())
	return err
}

func (c *Compressor) lastTime(id string) (int64, error) {
	sql := `SELECT end FROM compressed WHERE id = ? ORDER BY end DESC LIMIT 1`
	var end int64
	t0 := time.Now()
	err := c.cfg.Session.Query(sql, id).Scan(&end)
	c.fetchDuration.Observe(time.Since(t0).Seconds())
	return end, err
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
