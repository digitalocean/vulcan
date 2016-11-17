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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/model"
	"github.com/digitalocean/vulcan/querier"
)

// Resolver implements the querier.Resolver interface.
type Resolver struct {
	addrClients    map[string]ResolverClient
	cfg            *ResolverConfig
	m              sync.RWMutex
	partitionAddrs map[int32][]string
}

// ResolverConfig is necessary for creating a new Resolver.
type ResolverConfig struct {
	Client  sarama.Client
	Context context.Context
	GroupID string
	Topic   string
	Refresh time.Duration
}

// NewResolver creates and starts the background processes necessary.
func NewResolver(cfg *ResolverConfig) (*Resolver, error) {
	r := &Resolver{
		addrClients:    map[string]ResolverClient{},
		cfg:            cfg,
		partitionAddrs: map[int32][]string{},
	}
	err := r.fetchAndSet()
	if err != nil {
		return nil, err
	}
	go r.run()
	return r, nil
}

func (r *Resolver) Resolve(ctx context.Context, matchers []*querier.Match) ([]*model.TimeSeries, error) {
	next := make([]*Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		m := &Matcher{
			Name:  matcher.Name,
			Value: matcher.Value,
		}
		switch matcher.Type {
		case querier.Equal:
			m.Type = MatcherType_Equal
		case querier.NotEqual:
			m.Type = MatcherType_NotEqual
		case querier.RegexMatch:
			m.Type = MatcherType_RegexMatch
		case querier.RegexNoMatch:
			m.Type = MatcherType_RegexNoMatch
		default:
			panic("unhandled matcher type")
		}
		next = append(next, m)
	}
	wg := &sync.WaitGroup{}
	var outerError error
	r.m.RLock()
	m := &sync.Mutex{}
	result := make(map[int32][]string, len(r.partitionAddrs))
	wg.Add(len(r.partitionAddrs))
	for partition := range r.partitionAddrs {
		go func(partition int32) {
			defer wg.Done()
			ids, err := r.resolve(ctx, partition, next)
			if err != nil {
				outerError = err
				return
			}
			m.Lock()
			result[partition] = ids
			m.Unlock()
		}(partition)
	}
	r.m.RUnlock()
	wg.Wait()
	tsb := []*model.TimeSeries{}
	for _, part := range result {
		for _, id := range part {
			labels, err := model.LabelsFromTimeSeriesID(id)
			if err != nil {
				return []*model.TimeSeries{}, err
			}
			tsb = append(tsb, &model.TimeSeries{
				Labels: labels,
			})
		}
	}
	return tsb, nil
}

func (r *Resolver) Values(ctx context.Context, field string) ([]string, error) {
	return []string{}, nil
}

func (r *Resolver) client(partition int32) (ResolverClient, error) {
	r.m.RLock()
	addrs, ok := r.partitionAddrs[partition]
	r.m.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unhandled partition")
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses for partition")
	}
	// TODO smarter hostpool selection of address
	addr := addrs[0]
	r.m.RLock()
	c, ok := r.addrClients[addr]
	r.m.RUnlock()
	if ok {
		return c, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	newClient := NewResolverClient(conn)
	r.m.Lock()
	c, ok = r.addrClients[addr]
	if ok {
		r.m.Unlock()
		return c, nil
	}
	r.addrClients[addr] = newClient
	r.m.Unlock()
	// TODO timeout and remove clients.
	return newClient, nil
}

func (r *Resolver) resolve(ctx context.Context, partition int32, matchers []*Matcher) ([]string, error) {
	c, err := r.client(partition)
	if err != nil {
		return []string{}, err
	}
	req := &ResolveRequest{
		Partition: partition,
		Matchers:  matchers,
	}
	resp, err := c.Resolve(ctx, req)
	if err != nil {
		return []string{}, err
	}
	return resp.Ids, nil
}

func (r *Resolver) run() {
	t := time.NewTimer(0)
	for {
		select {
		case <-r.cfg.Context.Done():
			return
		case <-t.C:
			t.Reset(r.cfg.Refresh)
			err := r.fetchAndSet()
			if err != nil {
				logrus.WithError(err).Error("while fetching and setting resolver")
			}
		}
	}
}

func (r *Resolver) fetchAndSet() error {
	b, err := r.cfg.Client.Coordinator(r.cfg.GroupID)
	if err != nil {
		return err
	}
	partitions, err := r.cfg.Client.Partitions(r.cfg.Topic)
	if err != nil {
		return err
	}
	resp, err := b.DescribeGroups(&sarama.DescribeGroupsRequest{
		Groups: []string{r.cfg.GroupID},
	})
	if err != nil {
		return err
	}
	next := make(map[int32][]string, len(partitions))
	for _, partition := range partitions {
		next[partition] = []string{}
	}
	for _, group := range resp.Groups {
		if group.GroupId != r.cfg.GroupID {
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
				if topic != r.cfg.Topic {
					continue
				}
				for _, partition := range partitions {
					next[partition] = append(next[partition], us.AdvertisedAddr)
				}
			}
		}
	}
	r.m.Lock()
	r.partitionAddrs = next
	r.m.Unlock()
	return nil
}
