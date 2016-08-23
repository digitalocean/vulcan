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
	"errors"
	"testing"
	"time"
)

func TestWorkerRun(t *testing.T) {
	var runValidations = []struct {
		desc       string
		closeDelay time.Duration
		writerErr  error
		targetErr  error
	}{
		{
			desc:       "no errors",
			closeDelay: time.Duration(2) * time.Second,
		},
		{
			desc:       "fetch error",
			closeDelay: time.Duration(2) * time.Second,
			targetErr:  errors.New(TESTFETCHERR),
		},
		{
			desc:       "write errors",
			closeDelay: time.Duration(2) * time.Second,
			writerErr:  errors.New(TESTWRITEERR),
		},
	}

	for i, test := range runValidations {
		t.Logf("run validations %d: %q", i, test.desc)

		w := &Worker{
			key: "test.example.com",
			Target: &MockTargeter{
				i:                 time.Duration(30) * time.Second,
				fetchErr:          test.targetErr,
				numOfFetchMetrics: 1,
			},
			writer: &MockWriter{WErr: test.writerErr},
			done:   make(chan struct{}),
		}

		runCh := make(chan struct{})
		go func() {
			w.run()
			close(runCh)
		}()

		go func() {
			time.Sleep(test.closeDelay)
			w.Stop()
		}()

		select {
		case <-runCh:
			t.Logf("run validation %d: passed", i)
		case <-time.After(test.closeDelay + 300*time.Millisecond):
			t.Errorf(
				"run() => did not exit by expected time of %v",
				test.closeDelay,
			)
		}
	}
}
