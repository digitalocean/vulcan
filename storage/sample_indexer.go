package storage

import (
	"github.com/digitalocean/vulcan/bus"
	"github.com/prometheus/client_golang/prometheus"
)

type SampleIndexer interface {
	prometheus.Collector
	IndexSample(*bus.Sample) error
}
