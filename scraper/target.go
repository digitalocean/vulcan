package scraper

import (
	"time"

	dto "github.com/prometheus/client_model/go"
)

type Target interface {
	Equals(other Target) bool
	Fetch() ([]*dto.MetricFamily, error)
	Interval() time.Duration
}
