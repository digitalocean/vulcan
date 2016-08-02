package scraper

import (
	"time"

	dto "github.com/prometheus/client_model/go"
)

// Target is an interface that wraps the methods of exporter/job target.
type Target interface {
	Equals(other Target) bool
	Fetch() ([]*dto.MetricFamily, error)
	Interval() time.Duration
}
