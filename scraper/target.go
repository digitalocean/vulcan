package scraper

import (
	"time"

	dto "github.com/prometheus/client_model/go"
)

// Targeter is an interface that wraps the methods of exporter/job target.
type Targeter interface {
	Equals(other Targeter) bool
	// Fetch polls for metrics of the target.
	// It can pass off errors to a configurable endpoint.
	Fetch() ([]*dto.MetricFamily, error)
	Interval() time.Duration
	// Key returns unique key for target.
	Key() string
}

// Target represents a scrape target.
type Target struct {
	Job      string
	URL      string
	Instance string
	Interval time.Duration
}

// Key returns a unique key for the target
// TODO Need to consider a more robust method of generating a unique key.
func (t *Target) Key() string {
	return t.Job + t.URL
}
