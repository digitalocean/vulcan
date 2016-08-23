package kafka

import (
	"bytes"

	"github.com/Shopify/sarama"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
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
func (w *Writer) Write(key string, fams []*dto.MetricFamily) error {
	buf := &bytes.Buffer{}
	encoder := expfmt.NewEncoder(buf, expfmt.FmtText)
	for _, f := range fams {
		err := encoder.Encode(f)
		if err != nil {
			return err
		}
	}
	// set key to "${job}::${instance}" so that messages from the same job-instance consistently
	// go to the same kafka partition
	_, _, err := w.producer.SendMessage(&sarama.ProducerMessage{
		Topic: w.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(buf.Bytes()),
	})
	return err
}
