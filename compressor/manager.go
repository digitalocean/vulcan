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
	"errors"
	"math"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/consumer"
)

// ManagerConfig are the necessary components to run a Manager.
type ManagerConfig struct {
	Client      sarama.Client
	Coordinator *cg.Coordinator
	GroupID     string
	MaxAge      time.Duration
	MaxIdle     time.Duration
	Session     *gocql.Session
	TTL         time.Duration
}

// Manager connects to Kafka and ensures that each topic-partition that we are assigned is being
// consumed. If we fail to consume, we disconnect from kafka and allow other managers to take
// over those partitions.
type Manager struct {
	cfg              *ManagerConfig
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

// NewManager creates a Manager which will ensure that the compressor consumes its
// fair share of partitions and processes them.
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	return &Manager{
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
func (m *Manager) Describe(ch chan<- *prometheus.Desc) {
	m.fetchDuration.Describe(ch)
	m.flushUtilization.Describe(ch)
	m.highwatermark.Describe(ch)
	m.offset.Describe(ch)
	m.samplesTotal.Describe(ch)
	m.writeDuration.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *Manager) Collect(ch chan<- prometheus.Metric) {
	m.fetchDuration.Collect(ch)
	m.flushUtilization.Collect(ch)
	m.highwatermark.Collect(ch)
	m.offset.Collect(ch)
	m.samplesTotal.Collect(ch)
	m.writeDuration.Collect(ch)
}

// Run executes until completion or error.
func (m *Manager) Run() error {
	return m.cfg.Coordinator.Run(m.handle)
}

func (m *Manager) handle(ctx context.Context, topic string, partition int32) {
	log := logrus.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	})
	log.Info("taking control of topic-partition")
	defer log.Info("relenquishing control of topic-partition")
	count := 0
	backoff := time.NewTimer(time.Duration(0))
	for {
		count++
		select {
		case <-ctx.Done():
			return
		case <-backoff.C:
			// TODO set dur on exit so we can retry
			log.Info("consuming topic-partition")
			// TODO put this offset logic into kafka consumer
			b, err := m.cfg.Client.Coordinator(m.cfg.GroupID)
			if err != nil {
				log.WithError(err).Error("while getting group coordinator to get offset")
				continue
			}
			req := &sarama.OffsetFetchRequest{
				ConsumerGroup: m.cfg.GroupID,
				Version:       1,
			}
			req.AddPartition(topic, partition)
			resp, err := b.FetchOffset(req)
			if err != nil && err != sarama.ErrNoError {
				log.WithError(err).Error("while getting offset")
				continue
			}
			block := resp.GetBlock(topic, partition)
			if block == nil {
				err = errors.New("expected to get offset block")
				log.WithError(err).Error("expected to get offset block")
				continue
			}
			c, err := consumer.NewOffset(&consumer.OffsetConfig{
				CacheDuration: time.Second,
				Client:        m.cfg.Client,
				Context:       ctx,
				Coordinator:   m.cfg.Coordinator,
				Offset:        block.Offset,
				Partition:     partition,
				Topic:         topic,
			})
			if err != nil {
				log.WithError(err).Error("while creating consumer")
				continue
			}
			cmpr, err := NewCompressor(&CompressorConfig{
				Consumer: c,
				Context:  ctx,
			})
			if err != nil {
				log.WithError(err).Error("while createing compressor")
				continue
			}
			cmpr.Run()

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
