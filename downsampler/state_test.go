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
	"testing"

	"github.com/digitalocean/vulcan/model"
)

func TestUpdateLastWrite(t *testing.T) {
	var (
		initState = map[string]*int64{
			`{"a":"b"}`: int64ToPt(1),
			`{"c":"d"}`: int64ToPt(200),
		}

		ds = NewDownsampler(&Config{})

		inputFqmn = `{"a":"b"}`
		inputT    = int64(100)

		ch = make(chan struct{}, 1)
	)

	ds.lastWrite = initState

	go func() {
		// Send on unbuffered channel ensures cleanup starts before getLastWriteValue is called.
		ch <- struct{}{}
		ds.updateLastWrite(inputFqmn, inputT)
	}()

	<-ch
	got, ok := ds.getLastWriteValue(inputFqmn)
	if !ok {
		t.Fatalf(
			"expected to find fqmn of %q in lastWrites; found None",
			inputFqmn,
		)
	}

	if got != inputT {
		t.Errorf(
			"expected fmqn to have value of %d, but got %d",
			inputT,
			got,
		)
	}
}

func TestUpdateLastWrites(t *testing.T) {
	var (
		initState = map[string]*int64{
			`{"a":"b"}`: int64ToPt(1),
			`{"c":"d"}`: int64ToPt(200),
		}

		ds = NewDownsampler(&Config{})

		input = model.TimeSeriesBatch{
			&model.TimeSeries{
				Labels:  map[string]string{"a": "b"},
				Samples: []*model.Sample{&model.Sample{TimestampMS: 1000}},
			},
			&model.TimeSeries{
				Labels:  map[string]string{"c": "d"},
				Samples: []*model.Sample{&model.Sample{TimestampMS: 2000}},
			},
			&model.TimeSeries{
				Labels:  map[string]string{"e": "f"},
				Samples: []*model.Sample{&model.Sample{TimestampMS: 3000}},
			},
			&model.TimeSeries{
				Labels:  map[string]string{"g": "h"},
				Samples: []*model.Sample{&model.Sample{TimestampMS: 4000}},
			},
		}

		expected = []struct {
			getInput  string
			getOutput int
		}{
			{
				getInput:  `{"a":"b"}`,
				getOutput: 1000,
			},
			{
				getInput:  `{"c":"d"}`,
				getOutput: 2000,
			},
			{
				getInput:  `{"e":"f"}`,
				getOutput: 3000,
			},
			{
				getInput:  `{"g":"h"}`,
				getOutput: 4000,
			},
		}
	)

	ds.lastWrite = initState

	ds.updateLastWrites(input)

	for _, e := range expected {
		got, ok := ds.getLastWriteValue(e.getInput)
		if !ok {
			t.Errorf(
				"expected to find fqmn of %q in lastWrites; found None",
				e.getInput,
			)
		}

		if got != int64(e.getOutput) {
			t.Errorf(
				"expected fmqn to have value of %d, but got %d",
				e.getOutput,
				got,
			)
		}
	}
}

func TestCleanLastWrite(t *testing.T) {
	var (
		initState = map[string]*int64{
			`{"a":"b"}`: int64ToPt(100),
			`{"c":"d"}`: int64ToPt(200),
			`{"e":"f"}`: int64ToPt(300),
			`{"g":"h"}`: int64ToPt(400),
			`{"i":"j"}`: int64ToPt(500),
			`{"k":"l"}`: int64ToPt(600),
			`{"m":"n"}`: int64ToPt(700),
			`{"q":"r"}`: int64ToPt(800),
		}

		ds = NewDownsampler(&Config{})

		inputNow  = int64(900)
		inputDiff = int64(100)
	)

	ds.lastWrite = initState
	ds.cleanLastWrite(inputNow, inputDiff)

	gotLen := ds.lenLastWrite()
	if gotLen != 1 {
		t.Errorf(
			"expected lastWrite to have length of 1, but got %d",
			gotLen,
		)
	}

	expectedFqmn := `{"q":"r"}`
	got, ok := ds.getLastWriteValue(expectedFqmn)
	if !ok {
		t.Errorf(
			"expected to find fqmn of %q in lastWrites; found None",
			expectedFqmn,
		)
	}

	if got != int64(800) {
		t.Errorf(
			"expected fmqn to have value of 100, but got %d",
			got,
		)
	}
}
