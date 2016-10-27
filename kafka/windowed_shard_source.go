package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/digitalocean/vulcan/bus"
	cg "github.com/supershabam/sarama-cg"
)

type WindowedShardSourceConfig struct {
	Client         sarama.Client
	Ctx            context.Context
	GroupID        string
	Heartbeat      time.Duration
	SessionTimeout time.Duration
	Topic          string
	Window         time.Duration
}

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

func (wss *WindowedShardSource) Err() error {
	return wss.err
}

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
