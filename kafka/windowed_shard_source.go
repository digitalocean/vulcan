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
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/digitalocean/vulcan/bus"
	cg "github.com/supershabam/sarama-cg"
)

// WindowedShardSourceConfig is everything a WindowedShardSource needs to
// run healthy and strong.
type WindowedShardSourceConfig struct {
	Client         sarama.Client
	Ctx            context.Context
	GroupID        string
	Heartbeat      time.Duration
	SessionTimeout time.Duration
	Topic          string
	Window         time.Duration
}

// WindowedShardSource is a Shard Source that returns a channel of WindowedSource
// for each kafka partition it is responsible for.
type WindowedShardSource struct {
	cancel         func()
	client         sarama.Client
	ctx            context.Context
	ch             chan (<-chan *bus.SourcePayload)
	err            error
	groupID        string
	heartbeat      time.Duration
	sessionTimeout time.Duration
	topic          string
	window         time.Duration
}

// NewWindowedShardSource returns a WindowedShardSource if you provide a nice config.
func NewWindowedShardSource(cfg *WindowedShardSourceConfig) (*WindowedShardSource, error) {
	// TODO to cfg validation before just copying values into wss
	ctx, cancel := context.WithCancel(cfg.Ctx)
	wss := &WindowedShardSource{
		cancel:         cancel,
		client:         cfg.Client,
		ctx:            ctx,
		ch:             make(chan (<-chan *bus.SourcePayload)),
		groupID:        cfg.GroupID,
		heartbeat:      cfg.Heartbeat,
		sessionTimeout: cfg.SessionTimeout,
		topic:          cfg.Topic,
		window:         cfg.Window,
	}
	go func() {
		err := wss.run()
		if err != nil {
			wss.err = err
		}
		cancel()
	}()
	return wss, nil
}

// Err should be called after Messages() channel is closed and tells the caller if
// this source closed with an error or of natural causes.
func (wss *WindowedShardSource) Err() error {
	return wss.err
}

// Messages is a channel of channel of source payload. When this outter channel closes, the
// entire source is done. When a channel of source payload closes, we are no longer
// responsible for that partition in kafka, but we're still running.
func (wss *WindowedShardSource) Messages() <-chan (<-chan *bus.SourcePayload) {
	return wss.ch
}

func (wss *WindowedShardSource) run() error {
	defer close(wss.ch)
	// create kafka coordinator to determine what partitions we're responsible for.
	coord := cg.NewCoordinator(&cg.Config{
		Client:  wss.client,
		GroupID: wss.groupID,
		Protocols: []cg.ProtocolKey{
			{
				Protocol: &cg.HashRing{},
				Key:      "hashring",
			},
		},
		SessionTimeout: wss.sessionTimeout,
		Heartbeat:      wss.heartbeat,
		Topics:         []string{wss.topic},
		Consume:        wss.consume,
	})
	return coord.Run(wss.ctx)
}

func (wss *WindowedShardSource) consume(ctx context.Context, topic string, partition int32) {
	// coordinator consumer should return immediately. We are responsible for the
	// provided topic-partition until ctx says we're done.
	// TODO change coordinator API to call `go consume` on consume functions to ensure
	// that a bad implementor can't block. Worst case: we run two goroutined blocks.
	// It looks clunky for an implementor to go func() their code.
	go func() {
		ws, err := NewWindowedSource(&WindowedSourceConfig{
			Client:    wss.client,
			Ctx:       ctx,
			GroupID:   wss.groupID,
			Partition: partition,
			Topic:     topic,
			Window:    wss.window,
		})
		if err != nil {
			wss.err = err
			wss.cancel()
			return
		}
		ch := make(chan *bus.SourcePayload)
		select {
		case <-ctx.Done():
			return
		case wss.ch <- ch:
		}
		msgCh := ws.Messages()
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					err = ws.Err()
					if err != nil {
						wss.err = err
						wss.cancel()
					}
					return
				}
				select {
				case <-ctx.Done():
					return
				case ch <- msg:
				}
			}
		}
	}()
}
