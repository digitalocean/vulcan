package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
)

type BackfillConsumerConfig struct {
	Client    sarama.Client
	Ctx       context.Context
	Topic     string
	Partition int32
}

type BackfillConsumer struct {
	cfg    *BackfillConsumerConfig
	client sarama.Client
	ch     chan *sarama.ConsumerMessage
	ctx    context.Context
	err    error
}

func NewBackfillConsumer(cfg *BackfillConsumerConfig) (*BackfillConsumer, error) {
	bc := &BackfillConsumer{
		cfg:    cfg,
		client: cfg.Client,
		ch:     make(chan *sarama.ConsumerMessage),
	}
	go bc.run(cfg.Ctx)
	return bc, nil
}

func (bc *BackfillConsumer) run(ctx context.Context) {
	logrus.Debug("running")
	defer logrus.Debug("exiting run")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	csmr, err := sarama.NewConsumerFromClient(bc.cfg.Client)
	if err != nil {
		bc.err = err
		return
	}
	go func() {
		<-ctx.Done()
		csmr.Close()
	}()
	pc, err := csmr.ConsumePartition(bc.cfg.Topic, bc.cfg.Partition, sarama.OffsetOldest)
	if err != nil {
		bc.err = err
		return
	}
	msgCh := pc.Messages()
Outer:
	for {
		select {
		case <-ctx.Done():
			break Outer
		case m := <-msgCh:
			logrus.Debug("got message")
			select {
			case <-ctx.Done():
				break Outer
			case bc.ch <- m:
			}
		}
	}
	close(bc.ch)
}

func (bc *BackfillConsumer) Err() error {
	return bc.err
}

func (bc *BackfillConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return bc.ch
}
