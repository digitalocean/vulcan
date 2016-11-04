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
	"math"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	cg "github.com/supershabam/sarama-cg"
)

type Config struct {
	Client      sarama.Client
	Coordinator *cg.Coordinator
	MaxAge      time.Duration
}

type Crazy struct {
	cfg  *Config
	done chan struct{}
	err  error
}

func NewCrazy(cfg *Config) (*Crazy, error) {
	return &Crazy{
		cfg:  cfg,
		done: make(chan struct{}),
	}, nil
}

func (c *Crazy) consume(ctx context.Context, topic string, partition int32) error {
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
	for msg := range twc.Consume() {

	}
	return twc.Err()
}

func (c *Crazy) handle(ctx context.Context, topic string, partition int32) {
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

func (c *Crazy) run() {
	err := c.cfg.Coordinator.Run(c.handle)
	if err != nil {
		c.err = err
	}
	close(c.done)
}
