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
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/model"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/remote"
	cg "github.com/supershabam/sarama-cg"
)

// Config starts up your crazy face.
type Config struct {
	Client      sarama.Client
	Coordinator *cg.Coordinator
	MaxAge      time.Duration
}

// Crazy is an idea to run an in-memory kafka consumer of varbit compressed metrics
// and expose that data to the querier.
type Crazy struct {
	accs         map[string]*Accumulator
	cfg          *Config
	m            sync.RWMutex
	samplesTotal *prometheus.CounterVec
}

// NewCrazy creates a new crazy, but does not start it.
func NewCrazy(cfg *Config) (*Crazy, error) {
	return &Crazy{
		accs: map[string]*Accumulator{},
		cfg:  cfg,
		samplesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vulcan",
			Subsystem: "crazy",
			Name:      "samples_total",
			Help:      "count of samples ingested into crazy",
		}, []string{"topic", "partition"}),
	}, nil
}

// Describe implements prometheus.Collector.
func (c *Crazy) Describe(ch chan<- *prometheus.Desc) {
	c.samplesTotal.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *Crazy) Collect(ch chan<- prometheus.Metric) {
	c.samplesTotal.Collect(ch)
}

// ChunksAfter returns the chunks for the given id that occur after the provided unix timestamp in ms.
func (c *Crazy) ChunksAfter(id string, after int64) (bool, []chunk.Chunk) {
	c.m.RLock()
	defer c.m.RUnlock()
	if acc, ok := c.accs[id]; ok {
		return true, acc.ChunksAfter(after)
	}
	return false, []chunk.Chunk{}
}

func (c *Crazy) consume(ctx context.Context, topic string, partition int32) error {
	partitionStr := fmt.Sprintf("%d", partition)
	twc, err := cg.NewTimeWindowConsumer(&cg.TimeWindowConsumerConfig{
		CacheDuration: time.Minute,
		Client:        c.cfg.Client,
		Context:       ctx,
		Coordinator:   c.cfg.Coordinator,
		Partition:     partition,
		Topic:         topic,
		Window:        c.cfg.MaxAge,
	})
	if err != nil {
		return err
	}
	go func() {
		gc := time.NewTicker(time.Minute * 10)
		defer gc.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-gc.C:
				cutoff := time.Now().Add(-c.cfg.MaxAge).UnixNano() / int64(time.Millisecond)
				oldids := make([]string, 0, 1000)
				c.m.RLock()
				for id, acc := range c.accs {
					if acc.Last() < cutoff {
						oldids = append(oldids, id)
					}
				}
				c.m.RUnlock()
				if len(oldids) == 0 {
					continue
				}
				c.m.Lock()
				for _, id := range oldids {
					if acc, ok := c.accs[id]; ok && acc.Last() < cutoff {
						delete(c.accs, id)
					}
				}
				c.m.Unlock()
			}
		}
	}()
	for msg := range twc.Consume() {
		tsb, err := parseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		for i, ts := range tsb {
			id := ts.ID()
			if i == 0 && msg.Offset%10000 == 0 {
				logrus.WithField("offset", msg.Offset).WithField("id", id).Info("tracing ingested id")
			}
			// attempt to get existing accumulator with just read lock.
			c.m.RLock()
			acc, ok := c.accs[id]
			c.m.RUnlock()
			if !ok {
				// create an accumulator and try to set it.
				myacc, err := NewAccumulator(&AccumulatorConfig{
					MaxAge: c.cfg.MaxAge,
				})
				if err != nil {
					return err
				}
				c.m.Lock()
				racc, ok := c.accs[id]
				if !ok {
					c.accs[id] = myacc
					acc = myacc
				} else {
					acc = racc
				}
				c.m.Unlock()
			}
			for _, s := range ts.Samples {
				err := acc.Append(s)
				if err != nil {
					return err
				}
				c.samplesTotal.WithLabelValues(topic, partitionStr).Inc()
			}
		}
	}
	return twc.Err()
}

func (c *Crazy) handle(ctx context.Context, topic string, partition int32) {
	// silently only handle partition 0
	if partition != 0 {
		return
	}
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
				}).Info("done consuming")
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

// Run blocks until crazy is done or errors.
func (c *Crazy) Run() error {
	return c.cfg.Coordinator.Run(c.handle)
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
