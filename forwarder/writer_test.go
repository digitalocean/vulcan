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

package forwarder

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/digitalocean/vulcan/bus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

func getKeyHappyPath(t *testing.T) {
	var happyPathTests = []struct {
		desc     string
		arg      *remote.TimeSeries
		expected string
	}{
		{
			desc: "all expected labels present in beginning of labels slice",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
					&remote.LabelPair{Name: model.AddressLabel, Value: "sammy.example.com:9999"},
					&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
				},
			},
			expected: "foobar-inst8888",
		},
		{
			desc: "all expected labels present in end of labels slice",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
					&remote.LabelPair{Name: model.AddressLabel, Value: "sammy.example.com:9999"},
					&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
				},
			},
			expected: "foobar-inst8888",
		},
		{
			desc: "only job and address labels present in end of labels slice",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
					&remote.LabelPair{Name: model.AddressLabel, Value: "sammy.example.com:9999"},
				},
			},
			expected: "foobar-sammy.example.com:9999",
		},
		{
			desc: "only job and address labels present in beginning of labels slice",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
					&remote.LabelPair{Name: model.AddressLabel, Value: "sammy.example.com:9999"},
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
				},
			},
			expected: "foobar-sammy.example.com:9999",
		},
		{
			desc: "all expected labels present, but instance label before address label",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
					&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AddressLabel, Value: "sammy.example.com:9999"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
				},
			},
			expected: "foobar-inst8888",
		},
		{
			desc: "only job and instance labels",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
				},
			},
			expected: "foobar-inst8888",
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path tests %d: %q", i, test.desc)

		got, err := GetKey(test.arg)
		if err != nil {
			t.Fatalf(
				"getKey(%v) => error: %v; expected nil errors",
				test.arg,
				err,
			)
		}

		if got != test.expected {
			t.Errorf(
				"getKey(%v) => %q; expected %q",
				test.arg,
				got,
				test.expected,
			)
		}
	}
}

func getKeyNegative(t *testing.T) {
	var negativeTests = []struct {
		desc string
		arg  *remote.TimeSeries
	}{
		{
			desc: "no job label",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
					&remote.LabelPair{Name: model.AddressLabel, Value: "sammy.example.com:9999"},
					&remote.LabelPair{Name: model.InstanceLabel, Value: "inst8888"},
				},
			},
		},
		{
			desc: "no address and instance label",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
					&remote.LabelPair{Name: model.JobLabel, Value: "foobar"},
				},
			},
		},
		{
			desc: "no address, job, and instance label",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{
					&remote.LabelPair{Name: model.MetricsPathLabel, Value: "/metrics"},
					&remote.LabelPair{Name: model.MetricNameLabel, Value: "fake"},
					&remote.LabelPair{Name: model.AlertNameLabel, Value: "boo"},
				},
			},
		},
		{
			desc: "no labels",
			arg: &remote.TimeSeries{
				Labels: []*remote.LabelPair{},
			},
		},
	}

	for i, test := range negativeTests {
		t.Logf("negative test %d: %q", i, test.desc)

		if _, err := GetKey(test.arg); err == nil {
			t.Errorf("getKey(%v) => expected an error but got nil", test.arg)
		}
	}
}

func TestGetKey(t *testing.T) {
	var tests = []struct {
		name     string
		testFunc func(*testing.T)
	}{
		{
			name:     "happy",
			testFunc: getKeyHappyPath,
		},
		{
			name:     "neg",
			testFunc: getKeyNegative,
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.testFunc)
	}
}

func TestForwarderWrite(t *testing.T) {
	var integrationTests = []struct {
		desc               string
		arg                *remote.WriteRequest
		expectedWriteCount int
		expectedWriteArgs  []*bus.MockWriteArgs
	}{
		{
			desc: "3 TimeSeries metrics, 3 unique keys",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{
					// job a
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34566),
								TimestampMs: int64(64657774758),
							},
						},
					},
					// job b
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:8888",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_b",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(8.099498),
								TimestampMs: int64(783474387548),
							},
						},
					},
					// job c
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_c",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(2.34566),
								TimestampMs: int64(649957774758),
							},
						},
					},
				},
			},
			expectedWriteCount: 3,
			expectedWriteArgs: []*bus.MockWriteArgs{
				&bus.MockWriteArgs{
					Key: "scrape_test_a-sammy.example.com:9999",
					WriteRequest: &remote.WriteRequest{
						Timeseries: []*remote.TimeSeries{
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(1.34566),
										TimestampMs: int64(64657774758),
									},
								},
							},
						},
					},
				},
				&bus.MockWriteArgs{
					Key: "scrape_test_b-sammy.example.com:8888",
					WriteRequest: &remote.WriteRequest{
						Timeseries: []*remote.TimeSeries{
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:8888",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_b",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(8.099498),
										TimestampMs: int64(783474387548),
									},
								},
							},
						},
					},
				},
				&bus.MockWriteArgs{
					Key: "scrape_test_c-sammy.example.com:9999",
					WriteRequest: &remote.WriteRequest{
						Timeseries: []*remote.TimeSeries{
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_c",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(2.34566),
										TimestampMs: int64(649957774758),
									},
								},
							},
						},
					},
				},
			},
		},

		{
			desc: "5 TimeSeries metrics, 2 unique keys",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{
					// job a
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34566),
								TimestampMs: int64(64657774758),
							},
						},
					},
					// job b
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:8888",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_b",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(8.099498),
								TimestampMs: int64(783474387548),
							},
						},
					},
					// job a
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(9090.00000),
								TimestampMs: int64(47834793879875),
							},
						},
					},
					// job a
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34335),
								TimestampMs: int64(4356466256),
							},
							&remote.Sample{
								Value:       float64(1455.454646),
								TimestampMs: int64(4597568475847),
							},
						},
					},
					// job b
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:8888",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_b",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(8.099498),
								TimestampMs: int64(889848577448),
							},
						},
					},
				},
			},
			expectedWriteCount: 2,
			expectedWriteArgs: []*bus.MockWriteArgs{
				&bus.MockWriteArgs{
					Key: "scrape_test_a-sammy.example.com:9999",
					WriteRequest: &remote.WriteRequest{
						Timeseries: []*remote.TimeSeries{
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(1.34566),
										TimestampMs: int64(64657774758),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(9090.00000),
										TimestampMs: int64(47834793879875),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(1.34335),
										TimestampMs: int64(4356466256),
									},
									&remote.Sample{
										Value:       float64(1455.454646),
										TimestampMs: int64(4597568475847),
									},
								},
							},
						},
					},
				},
				&bus.MockWriteArgs{
					Key: "scrape_test_b-sammy.example.com:8888",
					WriteRequest: &remote.WriteRequest{
						Timeseries: []*remote.TimeSeries{
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:8888",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_b",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(8.099498),
										TimestampMs: int64(783474387548),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:8888",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_b",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(8.099498),
										TimestampMs: int64(889848577448),
									},
								},
							},
						},
					},
				},
			},
		},

		{
			desc: "5 TimeSeries metrics, 1 unique key",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34566),
								TimestampMs: int64(64657774758),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(8.099498),
								TimestampMs: int64(783474387548),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(9090.00000),
								TimestampMs: int64(47834793879875),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.AddressLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(1.34335),
								TimestampMs: int64(4356466256),
							},
							&remote.Sample{
								Value:       float64(1455.454646),
								TimestampMs: int64(4597568475847),
							},
						},
					},
					&remote.TimeSeries{
						Labels: []*remote.LabelPair{
							&remote.LabelPair{
								Name:  model.InstanceLabel,
								Value: "sammy.example.com:9999",
							},
							&remote.LabelPair{
								Name:  model.JobLabel,
								Value: "scrape_test_a",
							},
						},
						Samples: []*remote.Sample{
							&remote.Sample{
								Value:       float64(8.099498),
								TimestampMs: int64(889848577448),
							},
						},
					},
				},
			},
			expectedWriteCount: 1,
			expectedWriteArgs: []*bus.MockWriteArgs{
				&bus.MockWriteArgs{
					Key: "scrape_test_a-sammy.example.com:9999",
					WriteRequest: &remote.WriteRequest{
						Timeseries: []*remote.TimeSeries{
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(1.34566),
										TimestampMs: int64(64657774758),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(8.099498),
										TimestampMs: int64(783474387548),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(9090.00000),
										TimestampMs: int64(47834793879875),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.AddressLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(1.34335),
										TimestampMs: int64(4356466256),
									},
									&remote.Sample{
										Value:       float64(1455.454646),
										TimestampMs: int64(4597568475847),
									},
								},
							},
							&remote.TimeSeries{
								Labels: []*remote.LabelPair{
									&remote.LabelPair{
										Name:  model.InstanceLabel,
										Value: "sammy.example.com:9999",
									},
									&remote.LabelPair{
										Name:  model.JobLabel,
										Value: "scrape_test_a",
									},
								},
								Samples: []*remote.Sample{
									&remote.Sample{
										Value:       float64(8.099498),
										TimestampMs: int64(889848577448),
									},
								},
							},
						},
					},
				},
			},
		},

		{
			desc: "no timeseries data in WriteRequest",
			arg: &remote.WriteRequest{
				Timeseries: []*remote.TimeSeries{},
			},
			expectedWriteCount: 0,
			expectedWriteArgs:  make([]*bus.MockWriteArgs, 0),
		},
	}

	for i, test := range integrationTests {
		t.Logf("integration test %d: %q", i, test.desc)

		mw := bus.NewMockWriter()
		f := NewForwarder(&Config{Writer: mw})

		// Not using any of the returned values currently
		_ = f.Write(context.Background(), test.arg)

		// wait for write calls to complete
		f.wg.Done()
		f.wg.Wait()

		if mw.WriteCount != test.expectedWriteCount {
			t.Errorf(
				"Forwarder.Write(context, %v) => %d write counts; expected %d",
				test.arg,
				mw.WriteCount,
				test.expectedWriteCount,
			)
		}

		// Sort expected and got args slice by key name since there is no send order
		// guarantee.
		bus.ByMockWriterArgs(bus.WriteArgKey).Sort(test.expectedWriteArgs)
		bus.ByMockWriterArgs(bus.WriteArgKey).Sort(mw.Args)

		if !reflect.DeepEqual(mw.Args, test.expectedWriteArgs) {
			t.Errorf(
				"Forwarder.Write(context, %v) => %v;\nexpected %v",
				test.arg,
				mw.Args,
				test.expectedWriteArgs,
			)
		}
	}
}
