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
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/kafka"
	"github.com/digitalocean/vulcan/model"
	"github.com/prometheus/client_golang/prometheus"
)

// Session wraps CacherClient. It queries Kafka to determine
// what cacher rpc server addresses are available per partition and directs the
// rpc requst to an appropriate address based on the requested metric id.
type Session struct {
	addrClients    map[string]*grpc.ClientConn
	cancel         func()
	cfg            *SessionConfig
	ctx            context.Context
	m              sync.RWMutex
	partitionAddrs map[int32][]string
	errors         *prometheus.CounterVec
}

// SessionConfig is required to instantiate a Session.
type SessionConfig struct {
	Client  sarama.Client
	GroupID string
	Topic   string
	Refresh time.Duration
}

// NewSession creates and starts the background processes for keeping the session active.
func NewSession(cfg *SessionConfig) (*Session, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		addrClients:    map[string]*grpc.ClientConn{},
		cancel:         cancel,
		cfg:            cfg,
		ctx:            ctx,
		partitionAddrs: map[int32][]string{},
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "vulcan",
			Subsystem: "cacher_session",
			Name:      "errors_total",
			Help:      "Count of errors occuring in the session object of the cacher component",
		}, []string{"method"}),
	}
	err := s.fetchAndSet()
	if err != nil {
		return nil, err
	}
	go s.run()
	return s, nil
}

// Describe implements prometheus.Collector.
func (s *Session) Describe(ch chan<- *prometheus.Desc) {
	s.errors.Describe(ch)
}

// Collect implements prometheus.Collector.
func (s *Session) Collect(ch chan<- prometheus.Metric) {
	s.errors.Collect(ch)
}

// Chunks routes a grpc Chunks request to a cacher responsible for the partition the requested
// id belongs to.
func (s *Session) Chunks(ctx context.Context, in *ChunksRequest) (*ChunksResponse, error) {
	c, err := s.client(in.Id)
	if err != nil {
		return nil, err
	}
	return c.Chunks(ctx, in)
}

func (s *Session) run() {
	t := time.NewTicker(s.cfg.Refresh)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			err := s.fetchAndSet()
			if err != nil {
				s.errors.WithLabelValues("fetch_and_set").Inc()
				logrus.WithError(err).Error("while running fetch_and_set")
			}
		}
	}
}

func (s *Session) client(id string) (CacherClient, error) {
	l, err := model.LabelsFromTimeSeriesID(id)
	if err != nil {
		return nil, err
	}
	routingKey := kafka.Key(kafka.Job(l["job"]), kafka.Instance(l["instance"]))
	s.m.RLock()
	p := kafka.HashNegativeAndReflectInsanity(routingKey, len(s.partitionAddrs))
	addrs, ok := s.partitionAddrs[p]
	s.m.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown partition")
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no rpc address for partition")
	}
	// TODO smarter hostpool selection of address
	addr := addrs[0]
	s.m.RLock()
	c, ok := s.addrClients[addr]
	s.m.RUnlock()
	if ok {
		return NewCacherClient(c), nil
	}
	// TODO store all grpc dial options in kafka userdata.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	s.m.Lock()
	c, ok = s.addrClients[addr]
	if ok {
		s.m.Unlock()
		return NewCacherClient(c), nil
	}
	s.addrClients[addr] = conn
	s.m.Unlock()
	// TODO timeout and remove clients.
	return NewCacherClient(conn), nil
}

// Close releases the session resources.
func (s *Session) Close() error {
	s.cancel()
	s.m.Lock()
	for _, c := range s.addrClients {
		c.Close()
	}
	s.m.Unlock()
	return nil
}

func (s *Session) fetchAndSet() error {
	b, err := s.cfg.Client.Coordinator(s.cfg.GroupID)
	if err != nil {
		return err
	}
	partitions, err := s.cfg.Client.Partitions(s.cfg.Topic)
	if err != nil {
		return err
	}
	next := make(map[int32][]string, len(partitions))
	for _, partition := range partitions {
		next[partition] = []string{}
	}
	resp, err := b.DescribeGroups(&sarama.DescribeGroupsRequest{
		Groups: []string{s.cfg.GroupID},
	})
	if err != nil {
		return err
	}
	for _, group := range resp.Groups {
		if group.GroupId != s.cfg.GroupID {
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
				if topic != s.cfg.Topic {
					continue
				}
				for _, partition := range partitions {
					next[partition] = append(next[partition], us.AdvertisedAddr)
				}
			}
		}
	}
	s.m.Lock()
	s.partitionAddrs = next
	s.m.Unlock()
	return nil
}
