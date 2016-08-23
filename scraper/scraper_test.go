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
	"time"
)

func TestScraperStaticJobSet(t *testing.T) {
	var happyPathTests = []struct {
		desc        string
		runningJobs map[string]*Worker
		param       []Targeter
	}{
		{
			desc:        "no currently running workers, 3 new targets",
			runningJobs: map[string]*Worker{},
			param: []Targeter{
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "foobar.example.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "barfoo.example.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "raboof.example.com",
				},
			},
		},
		{
			desc: "no matching running jobs",
			runningJobs: map[string]*Worker{
				"barfoo.example.com": &Worker{
					key: "barfoo.example.com",
					Target: &MockTargeter{
						i: time.Duration(30) * time.Second,
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
			},
			param: []Targeter{
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "foobar.example.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "raboof.example.com",
				},
			},
		},
		{
			desc: "all matching running jobs",
			runningJobs: map[string]*Worker{
				"foobar.example.com": &Worker{
					key: "foobar.example.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "foobar.example.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
			},
			param: []Targeter{
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "foobar.example.com",
				},
			},
		},
		{
			desc: "some matching running jobs",
			runningJobs: map[string]*Worker{
				"foobar.example.com": &Worker{
					key: "foobar.example.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "foobar.example.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
				"barfoo.example.com": &Worker{
					key: "barfoo.example.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "barfoo.example.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
				"baroof.example.com": &Worker{
					key: "baroof.example.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "baroof.example.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
			},
			param: []Targeter{
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "foobar.example.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "oofrab.example.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "raboof.example.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "baroof.example.com",
				},
			},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path tests %d: %q", i, test.desc)

		s := &Scraper{
			Targeter: NewMockTargetWatcher(),
			Writer:   &MockWriter{},
			running:  test.runningJobs,
		}

		s.set(test.param)

		if len(s.running) != len(test.param) {
			t.Errorf(
				"set(%v) => got %d running workers; expected %d",
				test.param,
				len(s.running),
				len(test.param),
			)
		}

		for _, tgt := range test.param {
			worker, ok := s.running[tgt.Key()]
			if !ok {
				t.Errorf(
					"set(%v) => did not find &scraper.running key with job name",
					test.param,
				)
			}

			if worker.Target.Key() != tgt.Key() {
				t.Errorf(
					"set(%v) => got running with unexpected Targeter: (key: %q); expected: (key: %q)",
					test.param,
					worker.Target.Key(),
					tgt.Key(),
				)
			}
		}
	}

	// Only one negative condition right now, when an empty slice of Targeters
	// is passed.
	var negativeTests = []struct {
		desc        string
		runningJobs map[string]*Worker
		param       []Targeter
	}{
		{
			desc:        "no targets, no existing running workers",
			runningJobs: map[string]*Worker{},
			param:       []Targeter{},
		},
		{
			desc: "no targets, existing running workers",
			runningJobs: map[string]*Worker{
				"barfoo.example.com": &Worker{
					key: "barfoo.example.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "barfoo.example.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
			},
			param: []Targeter{},
		},
	}

	for i, test := range negativeTests {
		t.Logf("negative path tests %d: %q", i, test.desc)

		s := &Scraper{
			Targeter: NewMockTargetWatcher(),
			Writer:   &MockWriter{},
			running:  test.runningJobs,
		}

		s.set(test.param)

		if len(s.running) != 0 {
			t.Errorf("set(%v) => %d running workers; expected 0", test.param, len(s.running))
		}

	}

}
