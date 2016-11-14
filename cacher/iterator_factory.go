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
	"net/url"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/digitalocean/vulcan/convert"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/digitalocean/vulcan/model"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

// IteratorFactory is able to create SeriesIterator that talk to a IteratorFactory.
// This bridges the new series iterator prometheus interface to our older cassandra
// code that was built for a different interface. Eventually, a cassandra storage
// object should just be able to create iterators directly.
type IteratorFactory struct {
	cfg            *IteratorFactoryConfig
	partitionAddrs map[int32]string
	partitions     []int32
	m              sync.Mutex
}

// IteratorFactoryConfig is necessary to create a new IteratorFactory.
type IteratorFactoryConfig struct {
	Client  sarama.Client
	Context context.Context
	GroupID string
	Topic   string
	Refresh time.Duration
}

// NewIteratorFactory creates and starts the background process for an IteratorFactory.
func NewIteratorFactory(cfg *IteratorFactoryConfig) (*IteratorFactory, error) {
	itrf := &IteratorFactory{
		cfg:            cfg,
		partitionAddrs: map[int32]string{},
		partitions:     []int32{},
	}
	go itrf.run()
	return itrf, nil
}

func (itrf *IteratorFactory) run() error {
	t := time.NewTimer(0)
	for {
		select {
		case <-itrf.cfg.Context.Done():
			return nil
		case <-t.C:
			t.Reset(itrf.cfg.Refresh)
			err := itrf.fetchAndSet()
			if err != nil {
				return err
			}
		}
	}
}

func (itrf *IteratorFactory) fetchAndSet() error {
	itrf.m.Lock()
	defer itrf.m.Unlock()
	b, err := itrf.cfg.Client.Coordinator(itrf.cfg.GroupID)
	if err != nil {
		return err
	}
	partitions, err := itrf.cfg.Client.Partitions(itrf.cfg.Topic)
	if err != nil {
		return err
	}
	itrf.partitions = partitions
	resp, err := b.DescribeGroups(&sarama.DescribeGroupsRequest{
		Groups: []string{itrf.cfg.GroupID},
	})
	if err != nil {
		return err
	}
	for _, group := range resp.Groups {
		if group.GroupId != itrf.cfg.GroupID {
			continue
		}
		for _, member := range group.Members {
			meta, err := member.GetMemberMetadata()
			if err != nil {
				return err
			}
			var us model.UserData
			err = json.Unmarshal(meta.UserData, &us)
			if err != nil {
				return err
			}
			assg, err := member.GetMemberAssignment()
			if err != nil {
				return err
			}
			for topic, partitions := range assg.Topics {
				if topic != itrf.cfg.Topic {
					continue
				}
				for _, partition := range partitions {
					itrf.partitionAddrs[partition] = us.AdvertisedAddr
				}
			}
		}
	}
	return nil
}

// Iterator returns a new SeriesIterator.
func (itrf *IteratorFactory) Iterator(m metric.Metric, from, through pmodel.Time) (local.SeriesIterator, error) {
	ts := convert.MetricToTimeSeries(m)
	key := kafka.Key(kafka.Job(ts.Labels["job"]), kafka.Instance(ts.Labels["instance"]))
	p := kafka.HashNegativeAndReflectInsanity(key, len(itrf.partitions))
	itrf.m.Lock()
	addr, ok := itrf.partitionAddrs[p]
	itrf.m.Unlock()
	if !ok {
		return nil, fmt.Errorf("could not find host handling partition %d", p)
	}
	v := url.Values{}
	v.Set("id", ts.ID())
	u := &url.URL{
		Scheme:   "http",
		Host:     addr,
		Path:     "chunks",
		RawQuery: v.Encode(),
	}
	return NewSeriesIterator(&SeriesIteratorConfig{
		Metric: m,
		After:  from,
		Before: through,
		URL:    u,
	}), nil
}
