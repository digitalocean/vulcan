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

package kafka

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/digitalocean/vulcan/bus"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/storage/remote"
)

// SourceConfig is the details needed to create a connection to Kafka.
type SourceConfig struct {
	Addrs    []string
	ClientID string
	GroupID  string
	Topics   []string
}

// Source reads from Kafka to fulfil the bus.Source interface.
type Source struct {
	c *cluster.Consumer
	e error
	m chan *bus.SourcePayload
}

// NewSource creates and starts a Kafka source.
func NewSource(config *SourceConfig) (*Source, error) {
	c, err := cluster.NewConsumer(config.Addrs, config.GroupID, config.Topics, cluster.Config{
		ClientID: config.ClientID,
		Version:  sarama.V0_9_0_0, // we use Kafka group consumer >0.9 semantics
	})
	if err != nil {
		return nil, err
	}
	s := &Source{
		c: c,
		m: make(chan *bus.SourcePayload),
	}
}

// Error SHOULD ONLY be called AFTER the messages channel has closed.
// This lets the caller determine if the messages channel closed because
// of an error or completed.
func (s *Source) Error() error {
	return s.e
}

// Messages returns a readable channel of SourcePayload. The payloads'
// Ack function MUST be called after the caller is done processing the
// payload. The channel will be closed when the Source encounters an
// error or the stream finishes. The caller SHOULD call Error() after
// the channel closes to determine if the channel closed because of
// an error or not.
func (s *Source) Messages() <-chan *SourcePayload {
	return s.m
}

func (s *Source) run() {
	defer close(s.m)
	for m := range s.c.Messages() {
		tsb, err := parseTimeSeriesBatch(m.Value)
		if err != nil {
			s.e = err
			return
		}
		p := &bus.SourcePayload{
			TimeSeriesBatch: tsb,
			Ack: func() {
				s.c.MarkOffset(m, "")
			},
		}
		s.m <- p
	}
}

func parseTimeSeriesBatch(in []byte) (bus.TimeSeriesBatch, error) {
	wr := &remote.WriteRequest{}
	if err := proto.Unmarshal(in, wr); err != nil {
		return nil, err
	}
	tsb := make(bus.TimeSeriesBatch, 0, len(wr))
	for _, protots := range wr.Timeseries {
		ts := &bus.TimeSeries{
			Labels:  map[string]string{},
			Samples: make([]*bus.Sample, 0, len(protots.Samples)),
		}
		for _, pair := range protots.Labels {
			ts.Labels[pair.Name] = pair.Value
		}
		for _, protosamp := range protots.Samples {
			ts.Samples = append(ts.Samples, &bus.Sample{
				TimestampMS: protosamp.TimestampMs,
				Value:       protosamp.Value,
			})
		}
		tsb = append(tsb, ts)
	}
	return tsb, nil
}
