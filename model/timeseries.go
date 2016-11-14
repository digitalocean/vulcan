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
	"sort"

	"github.com/prometheus/common/model"
)

// Sample is a single value at a time.
type Sample struct {
	TimestampMS int64
	Value       float64
}

// sampleSorter allows us to sort by different attributes of Sample.
// Implements sort.Interface.
type sampleSorter struct {
	samples []*Sample
	by      func(s1, s2 *Sample) bool
}

// Len implements sort.Interface.
func (s *sampleSorter) Len() int { return len(s.samples) }

// Swap implments sort.Interface.
func (s *sampleSorter) Swap(i, j int) {
	s.samples[i], s.samples[j] = s.samples[j], s.samples[i]
}

// Less implements sort.Interface.
func (s *sampleSorter) Less(i, j int) bool {
	return s.by(s.samples[i], s.samples[j])
}

// SampleSorter allows the sorting of a Sample slice by some attribute.
type SampleSorter func(s1, s2 *Sample) bool

// Sort sorts a Sample slice.
func (by SampleSorter) Sort(samples []*Sample) {
	s := &sampleSorter{samples: samples, by: by}

	sort.Sort(s)
}

// SortSampleByTS allows sorting by the sample collection time.  Alias of
// the Less method, which implements sort.Interface.
func SortSampleByTS(s1, s2 *Sample) bool {
	return s1.TimestampMS < s2.TimestampMS
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
