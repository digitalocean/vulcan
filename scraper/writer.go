package scraper

import (
	dto "github.com/prometheus/client_model/go"
)

// Writer is an interface that wraps the Write method to a message bus.
type Writer interface {
	Write(string, []*dto.MetricFamily) error
}
