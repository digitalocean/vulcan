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

package model

import (
	"encoding/json"

	"github.com/prometheus/common/model"
)

// Sample is a single value at a time.
type Sample struct {
	TimestampMS int64
	Value       float64
}

// TimeSeries is an identifying set of labels and one or more samples.
type TimeSeries struct {
	Labels  map[string]string
	Samples []*Sample
}

// Name returns the metric name for the TimeSeries stored as a special label.
func (ts *TimeSeries) Name() string {
	return ts.Labels[model.MetricNameLabel]
}

// ID is a consistent string based on the Labels of this TimeSeries that can
// also be parsed to recreate the original Labels. For this, we use JSON and
// leverage golang sorting map keys while marshalling to JSON:
// https://github.com/golang/go/blob/release-branch.go1.7/src/encoding/json/encode.go#L121-L127
func (ts *TimeSeries) ID() string {
	b, err := json.Marshal(ts.Labels)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// TimeSeriesBatch is a group of TimeSeries that are processed together
// for performance reasons.
type TimeSeriesBatch []*TimeSeries

// LabelsFromTimeSeriesID parses the Labels for a TimeSeries from a TimeSeries ID.
func LabelsFromTimeSeriesID(id string) (map[string]string, error) {
	l := map[string]string{}
	err := json.Unmarshal([]byte(id), &l)
	return l, err
}
