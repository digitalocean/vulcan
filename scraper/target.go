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

type target struct {
	Job      string
	URL      string
	Instance string
	Interval time.Duration
}

func (t *target) key() string {
	return t.Job + t.URL
}
