package scraper

import (
	dto "github.com/prometheus/client_model/go"
)

const TESTWRITEERR = `WRITEERR`

type MockWriter struct {
	WErr error
}

func (w *MockWriter) Write(k string, m []*dto.MetricFamily) error {
	return w.WErr
}
