// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
