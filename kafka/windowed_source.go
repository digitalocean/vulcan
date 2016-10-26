package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/vulcan/bus"
)

// WindowedSourceConfig is the details needed to create a connection to Kafka.
type WindowedSourceConfig struct {
	Addrs    []string
	ClientID string
	Ctx      context.Context
	GroupID  string
	Target   time.Time
	Topics   []string
}

// WindowedSource reads from Kafka to fulfil the bus.Source interface.
type WindowedSource struct {
	cancel  func()
	client  sarama.Client
	ctx     context.Context
	err     error
	groupID string
	m       chan *bus.SourcePayload
	target  time.Time
}

func NewWindowedSource(config *WindowedSourceConfig) (*WindowedSource, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = config.ClientID
	cfg.Version = sarama.V0_10_0_0
	client, err := sarama.NewClient(config.Addrs, cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(config.Ctx)
	ws := &WindowedSource{
		cancel:  cancel,
		client:  client,
		ctx:     ctx,
		groupID: config.GroupID,
		m:       make(chan *bus.SourcePayload),
		target:  config.Target,
	}
	go ws.run()
	return ws, nil
}

func (ws *WindowedSource) Messages() <-chan *bus.SourcePayload {
	return ws.m
}

func (ws *WindowedSource) run() {
	defer close(ws.m)
	defer ws.client.Close()
	coord := cg.NewCoordinator(&cg.Config{
		Client:  ws.client,
		GroupID: ws.groupID,
		Protocols: []cg.ProtocolKey{
			{
				Protocol: &cg.HashRing{},
				Key:      "hashring",
			},
		},
		SessionTimeout: 30 * time.Second,
		Heartbeat:      3 * time.Second,
		Topics:         []string{"vulcan"},
		Consume: func(ctx context.Context, topic string, partition int32) {
			go func() {
				err := ws.consume(ctx, topic, partition)
				if err != nil {
					ws.err = err
					ws.cancel()
				}
			}()
		},
	})
	err := coord.Run(ws.ctx)
	if err != nil {
		ws.err = err
	}
	return
}

// now responsible for consuming this topic-partition until ctx ends.
func (ws *WindowedSource) consume(ctx context.Context, topic string, partition int32) error {
	logrus.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	}).Info("consuming")
	defer logrus.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	}).Info("done consuming")

	wc, err := NewWindowedConsumerFromClient(ws.client)
	if err != nil {
		return err
	}
	// TODO we probably don't want ws.target as a target time, we probably want a duration
	// and compute off now each time we call this.
	msgCh := wc.Consume(ctx, topic, partition, ws.target)
	noop := func() {}
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			ack := noop
			if msg.Timestamp.After(ws.target) {
				if msg.Offset%1000 == 0 {
					ack = func() {
						logrus.WithField("offset", msg.Offset).Info("TODO: actually mark offset")
					}
				}
			}
			tsb, err := parseTimeSeriesBatch(msg.Value)
			if err != nil {
				return err
			}
			sp := &bus.SourcePayload{
				TimeSeriesBatch: tsb,
				Ack:             ack,
			}
			select {
			case <-ctx.Done():
				return nil
			case ws.m <- sp:
			}
		}
	}
}

type WindowedConsumer struct {
	client sarama.Client
	err    error
}

func NewWindowedConsumerFromClient(client sarama.Client) (*WindowedConsumer, error) {
	return &WindowedConsumer{
		client: client,
	}, nil
}

func (wc *WindowedConsumer) Err() error {
	return wc.err
}

func (wc *WindowedConsumer) Consume(ctx context.Context, topic string, partition int32, target time.Time) <-chan *sarama.ConsumerMessage {
	out := make(chan *sarama.ConsumerMessage)
	go func() {
		defer close(out)
		offset, err := wc.binarySearch(topic, partition, target)
		if err != nil {
			wc.err = err
			return
		}
		c, err := sarama.NewConsumerFromClient(wc.client)
		if err != nil {
			wc.err = err
			return
		}
		defer c.Close()
		pc, err := c.ConsumePartition(topic, partition, offset)
		if err != nil {
			wc.err = err
			return
		}
		msgCh := pc.Messages()
		defer pc.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- msg:
				}
			}
		}
	}()
	return out
}

func (wc *WindowedConsumer) binarySearch(topic string, partition int32, target time.Time) (int64, error) {
	lower, upper, err := wc.bounds(topic, partition)
	if err != nil {
		return 0, err
	}
	// GetOffset can at-best return the segment the desired time starts in; it doesn't return an accurate offset.
	offset, err := wc.client.GetOffset(topic, partition, target.UnixNano()/int64(time.Millisecond))
	if err == sarama.ErrOffsetOutOfRange {
		// could not get time offset, falling back to mid offset.
		offset = (lower + upper) / 2
	}
	for offset != lower && offset != upper {
		t, err := wc.timeAt(topic, partition, offset)
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

func (wc *WindowedConsumer) bounds(topic string, partition int32) (lower, upper int64, err error) {
	lower, err = wc.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}
	upper, err = wc.client.GetOffset(topic, partition, sarama.OffsetNewest)
	return
}

func (wc *WindowedConsumer) timeAt(topic string, partition int32, offset int64) (time.Time, error) {
	c, err := sarama.NewConsumerFromClient(wc.client)
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
