package storage

import (
	"github.com/digitalocean/vulcan/bus"

	"github.com/prometheus/client_golang/prometheus"
)

// SampleIndexer is an interface that wraps the methods of a Prometheus
// Collector interface and IndexSample method.
type SampleIndexer interface {
	prometheus.Collector
	IndexSample(*bus.Sample) error
}
