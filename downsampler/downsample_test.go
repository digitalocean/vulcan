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

package downsampler

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/ingester"
	"github.com/digitalocean/vulcan/model"
)

func TestShouldWrite(t *testing.T) {
	var happyPathTests = []struct {
		name          string
		desc          string
		input         *model.TimeSeries
		outputBool    bool
		outputSample  *model.Sample
		resolution    time.Duration
		currLastWrite map[string]*int64
		readSamples   []*model.Sample
	}{
		{
			name: "a",
			desc: "brand new timeseries, not written recently",
			input: &model.TimeSeries{
				Labels: map[string]string{"a": "b"},
				Samples: []*model.Sample{
					&model.Sample{
						TimestampMS: int64(100000),
						Value:       float64(1),
					},
					&model.Sample{
						TimestampMS: int64(100015),
						Value:       float64(2),
					},
				},
			},
			outputBool: true,
			outputSample: &model.Sample{
				TimestampMS: int64(100015),
				Value:       float64(2),
			},
			resolution:    time.Duration(15 * time.Minute),
			currLastWrite: map[string]*int64{},
			readSamples: []*model.Sample{
				&model.Sample{TimestampMS: 0, Value: 0},
			},
		},
		{
			name: "b",
			desc: "timeseries in hash state, not written recently",
			input: &model.TimeSeries{
				Labels: map[string]string{"a": "b"},
				Samples: []*model.Sample{
					&model.Sample{
						TimestampMS: int64(100000),
						Value:       float64(1),
					},
					&model.Sample{
						TimestampMS: int64(100015),
						Value:       float64(2),
					},
				},
			},
			outputBool: true,
			outputSample: &model.Sample{
				TimestampMS: int64(100015),
				Value:       float64(2),
			},
			resolution: time.Duration(1 * time.Minute),
			currLastWrite: map[string]*int64{
				`{"a":"b"}`: int64ToPt(1),
			},
			readSamples: []*model.Sample{},
		},
		{
			name: "c ",
			desc: "timeseries in hash state, written recently",
			input: &model.TimeSeries{
				Labels: map[string]string{"a": "b"},
				Samples: []*model.Sample{
					&model.Sample{
						TimestampMS: int64(100000),
						Value:       float64(1),
					},
					&model.Sample{
						TimestampMS: int64(100015),
						Value:       float64(2),
					},
				},
			},
			outputBool:   false,
			outputSample: nil,
			resolution:   time.Duration(1 * time.Minute),
			currLastWrite: map[string]*int64{
				`{"a":"b"}`: int64ToPt(80000),
			},
			readSamples: []*model.Sample{},
		},
		{
			name: "d",
			desc: "brand new timeseries, written recently",
			input: &model.TimeSeries{
				Labels: map[string]string{"a": "b"},
				Samples: []*model.Sample{
					&model.Sample{
						TimestampMS: int64(100000),
						Value:       float64(1),
					},
					&model.Sample{
						TimestampMS: int64(100015),
						Value:       float64(2),
					},
				},
			},
			outputBool:    false,
			outputSample:  nil,
			resolution:    time.Duration(1 * time.Minute),
			currLastWrite: map[string]*int64{},
			readSamples: []*model.Sample{
				&model.Sample{TimestampMS: 80000, Value: 0},
			},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path test %d: %q", i, test.desc)

		t.Run(test.name, func(t *testing.T) {
			ds := NewDownsampler(&Config{
				Writer:     &ingester.MockWriter{},
				Reader:     &cassandra.MockReader{Samples: test.readSamples},
				Resolution: test.resolution,
			})

			ds.lastWrite = test.currLastWrite

			gotBool, gotSample, err := ds.shouldWrite(test.input)
			if err != nil {
				t.Fatalf(
					"shouldWrite(%v) => _, error: %v; expected nil errors",
					test.input,
					err,
				)
			}

			if gotBool != test.outputBool || !reflect.DeepEqual(gotSample, test.outputSample) {
				t.Errorf(
					"shouldWrite(%v) => %v, %v; expected %v, %v",
					test.input,
					gotBool,
					gotSample,
					test.outputBool,
					test.outputSample,
				)
			}
		})
	}

	var negativeTests = []struct {
		name          string
		desc          string
		input         *model.TimeSeries
		outputBool    bool
		outputSample  *model.Sample
		resolution    time.Duration
		currLastWrite map[string]*int64
		readSamples   []*model.Sample
		readErr       error
	}{
		{
			name: "a",
			desc: "brand new timeseries, not written recently",
			input: &model.TimeSeries{
				Labels: map[string]string{"a": "b"},
				Samples: []*model.Sample{
					&model.Sample{
						TimestampMS: int64(100000),
						Value:       float64(1),
					},
					&model.Sample{
						TimestampMS: int64(100015),
						Value:       float64(2),
					},
				},
			},
			resolution:    time.Duration(15 * time.Minute),
			currLastWrite: map[string]*int64{},
			readSamples: []*model.Sample{
				&model.Sample{TimestampMS: 0, Value: 0},
			},
			readErr: errors.New("read error"),
		},
	}

	for i, test := range negativeTests {
		t.Logf("negative path tests %d: %q", i, test.desc)

		t.Run(test.name, func(t *testing.T) {
			ds := NewDownsampler(&Config{
				Writer: &ingester.MockWriter{},
				Reader: &cassandra.MockReader{
					Samples: test.readSamples,
					Err:     test.readErr,
				},
				Resolution: test.resolution,
			})

			ds.lastWrite = test.currLastWrite

			if _, _, err := ds.shouldWrite(test.input); err == nil {
				t.Fatalf(
					"shouldWrite(%v) => _, nil error; expected an error",
					test.input,
				)
			}
		})
	}
}
