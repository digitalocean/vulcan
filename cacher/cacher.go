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

package cacher

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/digitalocean/vulcan/model"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/remote"
	cg "github.com/supershabam/sarama-cg"
	cgconsumer "github.com/supershabam/sarama-cg/consumer"
)

// Config is necessary for creating a new Cacher.
type Config struct {
	Cleanup     time.Duration
	Client      sarama.Client
	Coordinator *cg.Coordinator
	MaxAge      time.Duration
	Topic       string
}

type consumer struct {
	Accs map[string]*Accumulator
	M    sync.RWMutex
}

// Cacher is an idea to run an in-memory kafka consumer of varbit compressed metrics
// and expose that data to the querier.
type Cacher struct {
	cfg           *Config
	consumers     map[int32]*consumer
	m             sync.RWMutex
	numPartitions int
	samplesTotal  *prometheus.CounterVec
}

// NewCacher creates but doesn't start a Cacher. Call Run to begin.
func NewCacher(cfg *Config) (*Cacher, error) {
	partitions, err := cfg.Client.Partitions(cfg.Topic)
	if err != nil {
		return nil, err
	}
	return &Cacher{
		cfg:           cfg,
		consumers:     map[int32]*consumer{},
		numPartitions: len(partitions),
		samplesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vulcan",
			Subsystem: "cacher",
			Name:      "samples_total",
			Help:      "count of samples ingested into cacher",
		}, []string{"topic", "partition"}),
	}, nil
}

// Describe implements prometheus.Collector.
func (c *Cacher) Describe(ch chan<- *prometheus.Desc) {
	c.samplesTotal.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *Cacher) Collect(ch chan<- prometheus.Metric) {
	c.samplesTotal.Collect(ch)
}

// ServeHTTP allows the cacher to be attached to an http server.
func (c *Cacher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	chunks, err := c.ChunksAfter(id, time.Now().Add(-time.Hour*24*365).UnixNano()/int64(time.Millisecond))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := &chunksResp{}
	for _, chnk := range chunks {
		buf := make([]byte, chunk.ChunkLen)
		err := chnk.MarshalToBuf(buf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp.Chunks = append(resp.Chunks, buf)
	}
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	err = enc.Encode(resp)
	if err != nil {
		logrus.WithError(err).Info("while encoding response to client")
	}
}

// ChunksAfter returns the chunks for the given id that occur after the provided unix timestamp in ms.
func (c *Cacher) ChunksAfter(id string, after int64) ([]chunk.Chunk, error) {
	labels, err := model.LabelsFromTimeSeriesID(id)
	if err != nil {
		return nil, err
	}
	key := kafka.Key(kafka.Job(labels["job"]), kafka.Instance(labels["instance"]))
	p := kafka.HashNegativeAndReflectInsanity(key, c.numPartitions)
	c.m.RLock()
	cons, ok := c.consumers[p]
	c.m.RUnlock()
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	cons.M.RLock()
	acc, ok := cons.Accs[id]
	cons.M.RUnlock()
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return acc.ChunksAfter(after), nil
}

func (c *Cacher) consume(ctx context.Context, topic string, partition int32) error {
	cons := &consumer{
		Accs: map[string]*Accumulator{},
	}
	c.m.Lock()
	c.consumers[partition] = cons
	c.m.Unlock()
	defer func() {
		c.m.Lock()
		delete(c.consumers, partition)
		c.m.Unlock()
	}()
	partitionStr := fmt.Sprintf("%d", partition)
	twc, err := cgconsumer.NewTimeWindow(&cgconsumer.TimeWindowConfig{
		CacheDuration: time.Minute,
		Client:        c.cfg.Client,
		Context:       ctx,
		Coordinator:   c.cfg.Coordinator,
		Start:         cgconsumer.OffsetNewest,
		Partition:     partition,
		Topic:         topic,
		Window:        c.cfg.MaxAge,
	})
	if err != nil {
		return err
	}
	go func() {
		gc := time.NewTicker(c.cfg.Cleanup)
		defer gc.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-gc.C:
				cutoff := time.Now().Add(-c.cfg.MaxAge).UnixNano() / int64(time.Millisecond)
				// buffer of oldids to remove later with the write lock.
				oldids := make([]string, 0, 1000)
				cons.M.RLock()
				for id, acc := range cons.Accs {
					if acc.Last() < cutoff {
						oldids = append(oldids, id)
					}
				}
				cons.M.RUnlock()
				if len(oldids) == 0 {
					continue
				}
				cons.M.Lock()
				for _, id := range oldids {
					if acc, ok := cons.Accs[id]; ok && acc.Last() < cutoff {
						delete(cons.Accs, id)
					}
				}
				cons.M.Unlock()
			}
		}
	}()
	for msg := range twc.Consume() {
		tsb, err := parseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		for _, ts := range tsb {
			id := ts.ID()
			// attempt to get existing accumulator with just read lock.
			cons.M.RLock()
			acc, ok := cons.Accs[id]
			cons.M.RUnlock()
			if !ok {
				// create an accumulator and try to set it.
				myacc, err := NewAccumulator(&AccumulatorConfig{
					MaxAge: c.cfg.MaxAge,
				})
				if err != nil {
					return err
				}
				cons.M.Lock()
				racc, ok := cons.Accs[id]
				if !ok {
					cons.Accs[id] = myacc
					acc = myacc
				} else {
					acc = racc
				}
				cons.M.Unlock()
			}
			for _, s := range ts.Samples {
				err := acc.Append(s)
				if err != nil {
					return err
				}
				c.samplesTotal.WithLabelValues(topic, partitionStr).Inc()
			}
		}
		err = twc.CommitOffset(msg.Offset)
		if err != nil {
			return err
		}
	}
	return twc.Err()
}

func (c *Cacher) handle(ctx context.Context, topic string, partition int32) {
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

// Run blocks until cacher is done or errors.
func (c *Cacher) Run() error {
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
