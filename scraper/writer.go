package scraper

import (
	dto "github.com/prometheus/client_model/go"
)

type Writer interface {
	Write(JobName, Instance, []*dto.MetricFamily) error
}
