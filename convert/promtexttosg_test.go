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

package convert

import (
	"reflect"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

func TestWriteRequestToSampleGroup(t *testing.T) {
	var happyPathTests = []struct {
		name     string
		desc     string
		arg      *remote.WriteRequest
		expected bus.SampleGroup
	}{
		{
			name: "a",
			desc: "1 timeseries set from wr with 5 samples",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34566),
								TimestampMs: int64(649957774751),
							},
							&remote.Sample{
								Value:       float64(2.34566),
								TimestampMs: int64(649957774752),
							},
							&remote.Sample{
								Value:       float64(3.34566),
								TimestampMs: int64(649957774753),
							},
							&remote.Sample{
								Value:       float64(4.34566),
								TimestampMs: int64(649957774754),
							},
							&remote.Sample{
								Value:       float64(5.34566),
								TimestampMs: int64(649957774755),
							},
						},
					},
				},
			},
			expected: bus.SampleGroup{
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(1.34566),
						Timestamp: bus.Timestamp(int64(649957774751)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(2.34566),
						Timestamp: bus.Timestamp(int64(649957774752)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(3.34566),
						Timestamp: bus.Timestamp(int64(649957774753)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(4.34566),
						Timestamp: bus.Timestamp(int64(649957774754)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(5.34566),
						Timestamp: bus.Timestamp(int64(649957774755)),
					},
				},
			},
		},

		{
			name: "b",
			desc: "5 timeseries set from wr with 1 sample each",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test1"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34566),
								TimestampMs: int64(649957774751),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test2"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(2.34566),
								TimestampMs: int64(649957774752),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test3"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(3.34566),
								TimestampMs: int64(649957774753),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test4"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(4.34566),
								TimestampMs: int64(649957774754),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test5"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(5.34566),
								TimestampMs: int64(649957774755),
							},
						},
					},
				},
			},
			expected: bus.SampleGroup{
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test1",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test1",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(1.34566),
						Timestamp: bus.Timestamp(int64(649957774751)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test2",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test2",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(2.34566),
						Timestamp: bus.Timestamp(int64(649957774752)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test3",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test3",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(3.34566),
						Timestamp: bus.Timestamp(int64(649957774753)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test4",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test4",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(4.34566),
						Timestamp: bus.Timestamp(int64(649957774754)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test5",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test5",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(5.34566),
						Timestamp: bus.Timestamp(int64(649957774755)),
					},
				},
			},
		},

		{
			name: "c",
			desc: "5 timeseries set from wr with 3 samples each",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test1"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34566),
								TimestampMs: int64(649957774751),
							},
							&remote.Sample{
								Value:       float64(1.44566),
								TimestampMs: int64(749957774751),
							},
							&remote.Sample{
								Value:       float64(1.54566),
								TimestampMs: int64(849957774751),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test2"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(2.34566),
								TimestampMs: int64(649957774752),
							},
							&remote.Sample{
								Value:       float64(2.44566),
								TimestampMs: int64(749957774752),
							},
							&remote.Sample{
								Value:       float64(2.54566),
								TimestampMs: int64(849957774752),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test3"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(3.34566),
								TimestampMs: int64(649957774753),
							},
							&remote.Sample{
								Value:       float64(3.44566),
								TimestampMs: int64(749957774753),
							},
							&remote.Sample{
								Value:       float64(3.54566),
								TimestampMs: int64(849957774753),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test4"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(4.34566),
								TimestampMs: int64(649957774754),
							},
							&remote.Sample{
								Value:       float64(4.44566),
								TimestampMs: int64(749957774754),
							},
							&remote.Sample{
								Value:       float64(4.54566),
								TimestampMs: int64(849957774754),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
							&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
							&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
							&remote.LabelPair{Name: model.MetricNameLabel, Value: "sammy_test5"},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(5.34566),
								TimestampMs: int64(649957774755),
							},
							&remote.Sample{
								Value:       float64(5.44566),
								TimestampMs: int64(749957774755),
							},
							&remote.Sample{
								Value:       float64(5.54566),
								TimestampMs: int64(849957774755),
							},
						},
					},
				},
			},
			expected: bus.SampleGroup{
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test1",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test1",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(1.34566),
						Timestamp: bus.Timestamp(int64(649957774751)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test1",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test1",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(1.44566),
						Timestamp: bus.Timestamp(int64(749957774751)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test1",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test1",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(1.54566),
						Timestamp: bus.Timestamp(int64(849957774751)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test2",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test2",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(2.34566),
						Timestamp: bus.Timestamp(int64(649957774752)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test2",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test2",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(2.44566),
						Timestamp: bus.Timestamp(int64(749957774752)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test2",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test2",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(2.54566),
						Timestamp: bus.Timestamp(int64(849957774752)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test3",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test3",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(3.34566),
						Timestamp: bus.Timestamp(int64(649957774753)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test3",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test3",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(3.44566),
						Timestamp: bus.Timestamp(int64(749957774753)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test3",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test3",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(3.54566),
						Timestamp: bus.Timestamp(int64(849957774753)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test4",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test4",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(4.34566),
						Timestamp: bus.Timestamp(int64(649957774754)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test4",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test4",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(4.44566),
						Timestamp: bus.Timestamp(int64(749957774754)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test4",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test4",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(4.54566),
						Timestamp: bus.Timestamp(int64(849957774754)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test5",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test5",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(5.34566),
						Timestamp: bus.Timestamp(int64(649957774755)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test5",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test5",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(5.44566),
						Timestamp: bus.Timestamp(int64(749957774755)),
					},
				},
				&bus.Sample{
					Metric: bus.Metric{
						Name: "sammy_test5",
						Labels: map[string]string{
							model.MetricsPathLabel: "/metrics",
							model.AlertNameLabel:   "boo",
							model.InstanceLabel:    "inst8888",
							model.MetricNameLabel:  "sammy_test5",
						},
					},
					Datapoint: bus.Datapoint{
						Value:     float64(5.54566),
						Timestamp: bus.Timestamp(int64(849957774755)),
					},
				},
			},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path test %d, id %s: %q", i, test.name, test.desc)

		tc := &testWriteRequestToSampleGroup{
			input:    test.arg,
			expected: test.expected,
		}

		t.Run(test.name, tc.validate)
	}
}

type testWriteRequestToSampleGroup struct {
	input    *remote.WriteRequest
	expected bus.SampleGroup
}

func (tc *testWriteRequestToSampleGroup) validate(t *testing.T) {
	got := writeRequestToSampleGroup(tc.input)

	if !reflect.DeepEqual(got, tc.expected) {
		t.Errorf(
			"writeRequestToSampleGroup(%v) => %v; expected %v",
			tc.input,
			got,
			tc.expected,
		)
	}
}

func TestCheckTimestamp(t *testing.T) {
	var tests = []struct {
		name     string
		desc     string
		input    *remote.Sample
		expected bus.Timestamp
	}{
		{
			name: "a",
			desc: "valid timestamp in remote.Sample",
			input: &remote.Sample{
				Value:       float64(5.34566),
				TimestampMs: int64(649957774755),
			},
			expected: bus.Timestamp(649957774755),
		},
		{
			name: "b",
			desc: "missing timestamp in remote.Sample",
			input: &remote.Sample{
				Value: float64(5.34566),
			},
			expected: bus.Timestamp(time.Now().UnixNano() / 1e6),
		},
	}

	for i, test := range tests {
		t.Logf("test %d: %q", i, test.desc)

		tc := &testCheckTimestamp{
			input:    test.input,
			expected: test.expected,
		}

		t.Run(test.name, tc.validate)
	}
}

type testCheckTimestamp struct {
	input    *remote.Sample
	expected bus.Timestamp
}

func (tc *testCheckTimestamp) validate(t *testing.T) {
	got := checkTimestamp(tc.input)

	if got-tc.expected > bus.Timestamp(1) {
		t.Errorf(
			"checkTimestamp(%v) => %d; expected %d",
			tc.input,
			got,
			tc.expected,
		)
	}
}
