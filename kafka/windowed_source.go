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
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/digitalocean/vulcan/bus"
)

// WindowedSourceConfig is the details needed to create a connection to Kafka.
type WindowedSourceConfig struct {
	Client    sarama.Client
	Ctx       context.Context
	GroupID   string
	Partition int32
	Topic     string
	Window    time.Duration
}

// WindowedSource reads from Kafka to fulfil the bus.Source interface.
type WindowedSource struct {
	cancel    func()
	ch        chan *bus.SourcePayload
	client    sarama.Client
	ctx       context.Context
	err       error
	groupID   string
	partition int32
	topic     string
	window    time.Duration
}

// NewWindowedSource creates a bus source that starts at some window delta behind
// its last known offset.
func NewWindowedSource(cfg *WindowedSourceConfig) (*WindowedSource, error) {
	// TODO validate config before writing it into windowed source
	ctx, cancel := context.WithCancel(cfg.Ctx)
	ws := &WindowedSource{
		cancel:    cancel,
		ch:        make(chan *bus.SourcePayload),
		client:    cfg.Client,
		ctx:       ctx,
		groupID:   cfg.GroupID,
		partition: cfg.Partition,
		topic:     cfg.Topic,
		window:    cfg.Window,
	}
	go func() {
		err := ws.run()
		if err != nil {
			ws.err = err
		}
		cancel()
	}()
	return ws, nil
}

// Err returns an error or nil and should be called after the Messages channel
// closes.
func (ws *WindowedSource) Err() error {
	return ws.err
}

// Messages returns the channel of bus payloads to read. It will close when the
// source encounters an error or is finished.
func (ws *WindowedSource) Messages() <-chan *bus.SourcePayload {
	return ws.ch
}

func (ws *WindowedSource) run() error {
	defer close(ws.ch)
	offset, err := ws.offset()
	if err != nil {
		return err
	}
	c, err := sarama.NewConsumerFromClient(ws.client)
	if err != nil {
		return err
	}
	defer c.Close()
	pc, err := c.ConsumePartition(ws.topic, ws.partition, offset)
	if err != nil {
		return err
	}
	defer pc.Close()
	msgCh := pc.Messages()
	noop := func() {}
	for {
		select {
		case <-ws.ctx.Done():
			return nil
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			// TODO create ack function that actually commits offsets after we catch up to current.
			ack := noop
			tsb, err := parseTimeSeriesBatch(msg.Value)
			if err != nil {
				return err
			}
			sp := &bus.SourcePayload{
				TimeSeriesBatch: tsb,
				Ack:             ack,
			}
			select {
			case <-ws.ctx.Done():
				return nil
			case ws.ch <- sp:
			}
		}
	}
}

func (ws *WindowedSource) offset() (int64, error) {
	target := time.Now().Add(-ws.window)
	return ws.binarySearch(ws.topic, ws.partition, target)
}

func (ws *WindowedSource) binarySearch(topic string, partition int32, target time.Time) (int64, error) {
	lower, upper, err := ws.bounds(topic, partition)
	if err != nil {
		return 0, err
	}
	// GetOffset can at-best return the segment the desired time starts in; it doesn't return an accurate offset.
	offset, err := ws.client.GetOffset(topic, partition, target.UnixNano()/int64(time.Millisecond))
	if err == sarama.ErrOffsetOutOfRange {
		// could not get time offset, falling back to mid offset.
		offset = (lower + upper) / 2
	}
	for offset != lower && offset != upper {
		t, err := ws.timeAt(topic, partition, offset)
		if err != nil {
			return 0, err
		}
		if t.After(target) {
			upper = offset
			offset = (lower + offset) / 2
			continue
		}
		lower = offset
		offset = (offset + upper) / 2
	}
	return offset, nil
}

func (ws *WindowedSource) bounds(topic string, partition int32) (lower, upper int64, err error) {
	lower, err = ws.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}
	upper, err = ws.client.GetOffset(topic, partition, sarama.OffsetNewest)
	return
}

func (ws *WindowedSource) timeAt(topic string, partition int32, offset int64) (time.Time, error) {
	c, err := sarama.NewConsumerFromClient(ws.client)
	if err != nil {
		return time.Time{}, err
	}
	defer c.Close()
	pc, err := c.ConsumePartition(topic, partition, offset)
	if err != nil {
		return time.Time{}, err
	}
	defer pc.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("deadline exceeded for getting time at offset")
		case msg := <-pc.Messages():
			return msg.Timestamp, nil
		}
	}
}
