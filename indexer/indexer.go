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

package indexer

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	netctx "golang.org/x/net/context"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/model"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/storage/remote"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/consumer"
)

const (
	namespace = "vulcan"
	subsystem = "indexer"
)

// Indexer allows the querier to discover what metrics should be involved in
// a PromQL query.
type Indexer struct {
	cfg     *Config
	indexes map[int32]*NameIndex
	m       sync.RWMutex
}

// Config is required to create a new indexer.
type Config struct {
	Client      sarama.Client
	Coordinator *cg.Coordinator
}

// NewIndexer returns a new indexer but does not start it.
func NewIndexer(cfg *Config) (*Indexer, error) {
	return &Indexer{
		cfg:     cfg,
		indexes: map[int32]*NameIndex{},
	}, nil
}

// Run blocks until complete or error.
func (i *Indexer) Run() error {
	return i.cfg.Coordinator.Run(i.handle)
}

// Resolve returns the unique timeseries IDs that match the provided matchers.
func (i *Indexer) Resolve(ctx netctx.Context, req *ResolveRequest) (*ResolveResponse, error) {
	i.m.RLock()
	idx, ok := i.indexes[req.Partition]
	i.m.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown partition requested")
	}
	ids, err := idx.Resolve(req.Matchers)
	if err != nil {
		return nil, err
	}
	return &ResolveResponse{
		Ids: ids,
	}, nil
}

// Values returns the unique label values associated with the provided label name.
func (i *Indexer) Values(netctx.Context, *ValuesRequest) (*ValuesResponse, error) {
	return &ValuesResponse{}, fmt.Errorf("not implemented")
}

func (i *Indexer) handle(ctx context.Context, topic string, partition int32) {
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
			err := i.consume(ctx, topic, partition)
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

func (i *Indexer) consume(ctx context.Context, topic string, partition int32) error {
	idx := NewNameIndex()
	i.m.Lock()
	i.indexes[partition] = idx
	i.m.Unlock()
	defer func() {
		i.m.Lock()
		delete(i.indexes, partition)
		i.m.Unlock()
	}()
	seek := func(topic string, partition int32) (int64, error) {
		return i.cfg.Client.GetOffset(topic, partition, sarama.OffsetNewest)
	}
	c, err := consumer.NewSeek(&consumer.SeekConfig{
		CacheDuration: time.Minute,
		Client:        i.cfg.Client,
		Context:       ctx,
		Coordinator:   i.cfg.Coordinator,
		Partition:     partition,
		SeekFn:        seek,
		Topic:         topic,
	})
	if err != nil {
		return err
	}
	for msg := range c.Consume() {
		tsb, err := parseTimeSeriesBatch(msg.Value)
		if err != nil {
			return err
		}
		for _, ts := range tsb {
			id := ts.ID()
			idx.Add(id, ts.Labels)
		}
	}
	return c.Err()
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
