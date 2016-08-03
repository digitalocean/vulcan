package kafka

import (
	"bytes"
	"fmt"

	"github.com/digitalocean/vulcan/scraper"

	"github.com/Shopify/sarama"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type Writer struct {
	producer sarama.SyncProducer
	topic    string
}

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

type WriterConfig struct {
	ClientID string
	Addrs    []string
	Topic    string
}

func (w Writer) Write(job scraper.JobName, instance scraper.Instance, fams []*dto.MetricFamily) error {
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
	key := fmt.Sprintf(`%s::%s`, job, instance)
	_, _, err := w.producer.SendMessage(&sarama.ProducerMessage{
		Topic: w.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(buf.Bytes()),
	})
	return err
}
