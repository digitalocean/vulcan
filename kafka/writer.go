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
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/storage/remote"
)

// Writer represents an object that encapsulates the behavior of a Kafka
// producter.
type Writer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewWriter creates a new instance of Writer.
func NewWriter(config *WriterConfig) (*Writer, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = config.ClientID
	cfg.Producer.Compression = sarama.CompressionGZIP
	producer, err := sarama.NewSyncProducer(config.Addrs, cfg)
	if err != nil {
		return nil, err
	}
	return &Writer{
		producer: producer,
		topic:    config.Topic,
	}, nil
}

// WriterConfig represents the configuration of a Writer object.
type WriterConfig struct {
	ClientID string
	Addrs    []string
	Topic    string
}

// Write sends metrics to the Kafka message bus.
func (w *Writer) Write(key string, req *remote.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	// set key to "${job}::${instance}" so that messages from the same job-instance consistently
	// go to the same kafka partition
	_, _, err = w.producer.SendMessage(&sarama.ProducerMessage{
		Topic: w.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	})
	return err
}
