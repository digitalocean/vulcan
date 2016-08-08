package scraper

import (
	"time"

	dto "github.com/prometheus/client_model/go"
)

const TESTFETCHERR = `FETCHERR`

type MockTargeter struct {
	i                 time.Duration
	fetchErr          error
	isEqual           bool
	numOfFetchMetrics int
	key               string
}

func (t *MockTargeter) Equals(other Targeter) bool {
	return t.isEqual
}

func (t *MockTargeter) Fetch() ([]*dto.MetricFamily, error) {
	if t.fetchErr != nil {
		return nil, t.fetchErr
	}

	m := make([]*dto.MetricFamily, t.numOfFetchMetrics, t.numOfFetchMetrics)
	for i := range m {
		m[i] = getTestMetricFamily(i)
	}

	return m, nil
}

func (t *MockTargeter) Interval() time.Duration {
	return t.i
}

func (t *MockTargeter) Key() string {
	return t.key
}

func getTestMetricFamily(n int) *dto.MetricFamily {
	return &dto.MetricFamily{}
}
