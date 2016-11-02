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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/model"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/storage/remote"
	cg "github.com/supershabam/sarama-cg"
)

// Config is necessary to create a compressor.
type Config struct {
	Client           sarama.Client
	Coordinator      *cg.Coordinator
	MaxDirtyDuration time.Duration
	MaxSampleDelta   time.Duration
	Window           time.Duration
	Writer           *cassandra.Writer
}

// Compressor reads from kafka and writes to cassandra varbit encoded chunks. It can resume where it
// left off, load balance between many compressors with minimal disruption when a rebalance happens.
type Compressor struct {
	cfg *Config
}

// NewCompressor creates a compressor but you must call Run on it to start.
func NewCompressor(cfg *Config) (*Compressor, error) {
	return &Compressor{
		cfg: cfg,
	}, nil
}

// Run runs the compressor until completion or an error.
func (c *Compressor) Run() error {
	return c.cfg.Coordinator.Run(c.consume)
}

func (c *Compressor) consume(ctx context.Context, topic string, partition int32) {
	if partition != 0 {
		return
	}
	logrus.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	}).Info("consuming")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			twc, err := cg.NewTimeWindowConsumer(&cg.TimeWindowConsumerConfig{
				CacheDuration: time.Minute,
				Client:        c.cfg.Client,
				Context:       ctx,
				Coordinator:   c.cfg.Coordinator,
				Partition:     partition,
				Topic:         topic,
				Window:        c.cfg.Window,
			})
			if err != nil {
				// TODO bubble up error
				logrus.WithError(err).Error("exiting consume because of error, though I'm still responsible for this topic-partition")
				return
			}
			err = c.read(twc)
			if err != nil {
				// TODO bubble up error
				logrus.WithError(err).Error("exiting consume because of error, though I'm still responsible for this topic-partition")
				return
			}
		}
	}
}

func (c *Compressor) read(consumer cg.Consumer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := consumer.Consume()
	m := &sync.Mutex{}
	accs := map[string]*Accumulator{}
	// read to completion since the consumer is triggered off a ctx to stop if we need to kill it.
	for msg := range ch {
		tsb, err := parseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		wg := &sync.WaitGroup{}
		for _, ts := range tsb {
			id := ts.ID()
			wg.Add(1)
			go func(id string, samples []*model.Sample) {
				defer wg.Done()
				m.Lock()
				if _, ok := accs[id]; !ok {
					acc, err := NewAccumulator(&AccumulatorConfig{
						Context: ctx,
						Flush: func(buf []byte, start, end int64) {
							logrus.WithField("id", id).Info("flushing")
							err := c.cfg.Writer.WriteCompressed(id, start, end, buf)
							if err != nil {
								// TODO handle this error better.
								logrus.WithError(err).Error("do something about this")
								return
							}
						},
						MaxDirtyDuration: c.cfg.MaxDirtyDuration,
						MaxSampleDelta:   c.cfg.MaxSampleDelta,
					})
					if err != nil {
						m.Unlock()
						logrus.WithError(err).Error("do something better about this")
						return
					}
					accs[id] = acc
				}
				acc := accs[id]
				m.Unlock()
				for _, s := range samples {
					err := acc.Append(*s)
					if err != nil {
						logrus.WithError(err).Error("do something about this")
						return
					}
				}
			}(id, ts.Samples)
		}
		wg.Wait()
		err = consumer.CommitOffset(msg.Offset)
		if err != nil {
			return err
		}
	}
	return consumer.Err()
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
