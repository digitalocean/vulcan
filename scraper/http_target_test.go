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
	"testing"

	"github.com/golang/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
)

func TestFetch(t *testing.T) {

}

func TestAnnotate(t *testing.T) {
	var tests = []struct {
		desc   string
		fam    []*dto.MetricFamily
		target Target
	}{
		{
			desc: "1 MetricFamily, 1 LabelPair",
			fam: []*dto.MetricFamily{
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("foo"),
									Value: proto.String("bar"),
								},
							},
						},
					},
				},
			},
			target: Target{
				Job: "somejob",
			},
		},
		{
			desc: "3 MetricFamily, 1 LabelPair",
			fam: []*dto.MetricFamily{
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("foo"),
									Value: proto.String("bar"),
								},
							},
						},
					},
				},
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("bar"),
									Value: proto.String("foo"),
								},
							},
						},
					},
				},
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("oof"),
									Value: proto.String("rab"),
								},
							},
						},
					},
				},
			},
			target: Target{
				Job: "somejob",
			},
		},
		{
			desc: "1 MetricFamily, 3 LabelPair",
			fam: []*dto.MetricFamily{
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("foo"),
									Value: proto.String("bar"),
								},
								&dto.LabelPair{
									Name:  proto.String("bar"),
									Value: proto.String("foo"),
								},
								&dto.LabelPair{
									Name:  proto.String("oof"),
									Value: proto.String("rab"),
								},
							},
						},
					},
				},
			},
			target: Target{
				Job: "somejob",
			},
		},
		{
			desc: "3 MetricFamily, 3 LabelPair",
			fam: []*dto.MetricFamily{
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("foo"),
									Value: proto.String("bar"),
								},
								&dto.LabelPair{
									Name:  proto.String("bar"),
									Value: proto.String("foo"),
								},
								&dto.LabelPair{
									Name:  proto.String("oof"),
									Value: proto.String("rab"),
								},
							},
						},
					},
				},
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("bar"),
									Value: proto.String("foo"),
								},
								&dto.LabelPair{
									Name:  proto.String("bar"),
									Value: proto.String("foo"),
								},
								&dto.LabelPair{
									Name:  proto.String("oof"),
									Value: proto.String("rab"),
								},
							},
						},
					},
				},
				&dto.MetricFamily{
					Metric: []*dto.Metric{
						&dto.Metric{
							Label: []*dto.LabelPair{
								&dto.LabelPair{
									Name:  proto.String("oof"),
									Value: proto.String("rab"),
								},
								&dto.LabelPair{
									Name:  proto.String("bar"),
									Value: proto.String("foo"),
								},
								&dto.LabelPair{
									Name:  proto.String("oof"),
									Value: proto.String("rab"),
								},
							},
						},
					},
				},
			},
			target: Target{
				Job: "somejob",
			},
		},
	}

	for i, test := range tests {
		t.Logf("tests %d: %q", i, test.desc)

		annotate(test.fam, test.target)
		for _, mf := range test.fam {
			for _, m := range mf.Metric {
				var found bool

				for _, l := range m.Label {
					if *l.Name == "job" {
						if *l.Value != test.target.Job {
							t.Errorf(
								"annotate(%v, %v) => job label with value %q; expected %q",
								test.fam,
								test.target,
								*l.Value,
								test.target.Job,
							)
						}

						found = true
					}
				}

				if !found {
					t.Errorf(
						"annotate(%v, %v) => did not find job label",
						test.fam,
						test.target,
					)
				}
			}
		}
	}
}
