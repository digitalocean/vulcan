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

package zookeeper

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	"github.com/samuel/go-zookeeper/zk"
)

func mapChildren(cn []string, conn Client, rootPath string) map[string]*chState {
	m := make(map[string]*chState, len(cn))

	for _, c := range cn {
		m[c] = &chState{
			pt: &PathTargeter{
				conn:  conn,
				path:  path.Join(rootPath, c),
				done:  make(chan struct{}),
				out:   make(chan []scraper.Job),
				mutex: new(sync.Mutex),
				jobs:  map[string]scraper.Job{},
			},
		}
		go m[c].pt.run()
	}

	return m
}

func TestTargeterRun(t *testing.T) {
	// TODO Add tests that validate when only a subset of child nodes receive
	// job updates.  As currently implemented, the tests only test an all or
	// nothing scenario of job event updates.
	var validateResults = func(targets []scraper.Targeter, expectedTargets []string) {
		if len(targets) != len(expectedTargets) {
			t.Errorf(
				"run() => %d targets; expected %d",
				len(targets),
				len(expectedTargets),
			)
		}

		for _, expected := range expectedTargets {
			var found bool
			for _, got := range targets {
				if strings.Contains(got.Key(), expected) {
					found = true
				}
			}

			if !found {
				t.Errorf(
					"run() => received targets did contain expected target key %q",
					expected,
				)
			}
		}
	}

	// validates the key for all new targets against expected targets
	var jobUpdateTest = []struct {
		desc            string
		children        []string
		expectedTargets map[string]struct{}
	}{
		{
			desc:     "3 zk child node receives job event",
			children: []string{"child-a", "child-b", "child-c"},
		},
		{
			desc:     "1 zk child node receives job event",
			children: []string{"child-a"},
		},
		{
			desc: "26 zk child node receives job event",
			children: []string{"child-a", "child-b", "child-c", "child-d", "child-e",
				"child-f", "child-g", "child-h", "child-i", "child-j", "child-k", "child-l",
				"child-m", "child-n", "child-o", "child-p", "child-r", "child-s", "child-t",
				"child-u", "child-v", "child-x", "child-y", "child-z"},
		},
	}

	for i, test := range jobUpdateTest {
		t.Logf("job update test %d: %q", i, test.desc)

		zkRoot := "/vulca/test/targeter"
		c := NewZKConn()
		c.GetEventChannel = make(chan zk.Event)
		c.Children = test.children
		c.Jobs = make(map[string]string, len(test.children))
		expectedTargets := []string{}

		for _, chPath := range test.children {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.localhost:9101
        - %s.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.localhost:9101", chPath),
				fmt.Sprintf("%s.example.com:8888", chPath),
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: map[string]*chState{},
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    new(sync.Mutex),
			done:     make(chan struct{}),
		}

		//close := make(chan struct{})
		go func() {
			tg.run()
		}()

		// Get initial targets
		targets := <-tg.Targets()
		validateResults(targets, expectedTargets)

		// Set up new targets
		expectedTargets = []string{}
		for _, chPath := range test.children {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.hostlocal:9101
        - %s.sammyshark.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.hostlocal:9101", chPath),
				fmt.Sprintf("%s.sammyshark.example.com:8888", chPath),
			)
		}

		// Send job update event
		for i := 0; i < len(test.children); i++ {
			c.SendGetEvent(1, zkRoot)
			targets = <-tg.Targets()
		}
		validateResults(targets, expectedTargets)

		tg.Stop()
	}

	var childrenUpdateTest = []struct {
		desc           string
		initChildren   []string
		updateChildren map[string]struct{}
	}{
		{
			desc:         "all new children",
			initChildren: []string{"child-a", "child-b", "child-c"},
			updateChildren: map[string]struct{}{"child-d": struct{}{}, "child-e": struct{}{},
				"child-f": struct{}{}},
		},
		{
			desc:         "some new children",
			initChildren: []string{"child-a", "child-b", "child-c"},
			updateChildren: map[string]struct{}{"child-a": struct{}{}, "child-e": struct{}{},
				"child-f": struct{}{}, "child-g": struct{}{}},
		},
		{
			desc: "even more new children, some old, some new",
			initChildren: []string{"child-a", "child-b", "child-c", "child-d", "child-f",
				"child-g", "child-h", "child-i", "child-j", "child-k", "child-l", "child-m",
				"child-n", "child-o", "child-p", "child-q", "child-r", "child-s", "child-t",
				"child-u", "child-v", "child-w", "child-x", "child-y", "child-z"},
			updateChildren: map[string]struct{}{"child-a": struct{}{}, "child-e": struct{}{},
				"child-f": struct{}{}, "child-g": struct{}{}, "child-k": struct{}{},
				"child-t": struct{}{}, "child-u": struct{}{}, "child-y": struct{}{},
				"child-z": struct{}{}},
		},
		{
			desc:         "no new children, unexpected zk behavior",
			initChildren: []string{"child-a", "child-b", "child-c"},
			updateChildren: map[string]struct{}{"child-a": struct{}{}, "child-b": struct{}{},
				"child-c": struct{}{}},
		},
		{
			desc: "all children removed",
			initChildren: []string{"child-a", "child-b", "child-c", "child-d", "child-f",
				"child-g", "child-h", "child-i", "child-j", "child-k", "child-l", "child-m",
				"child-n", "child-o", "child-p", "child-q", "child-r", "child-s", "child-t",
				"child-u", "child-v", "child-w", "child-x", "child-y", "child-z"},
			updateChildren: map[string]struct{}{},
		},
	}

	for i, test := range childrenUpdateTest {
		t.Logf("children update test %d: %q", i, test.desc)

		zkRoot := "/vulcan/test/targeter"
		c := NewZKConn()
		c.ChildrenEventChannel = make(chan zk.Event)
		c.Children = test.initChildren
		c.Jobs = map[string]string{}
		expectedTargets := []string{}

		for _, chPath := range test.initChildren {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.localhost:9101
        - %s.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.localhost:9101", chPath),
				fmt.Sprintf("%s.example.com:8888", chPath),
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: map[string]*chState{},
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    &sync.Mutex{},
			done:     make(chan struct{}),
		}

		go func() {
			tg.run()
		}()

		// Get initial targets.
		targets := <-tg.Targets()
		validateResults(targets, expectedTargets)

		// Set up new targets.
		newChildren := make([]string, 0, len(test.updateChildren))
		for c := range test.updateChildren {
			newChildren = append(newChildren, c)
		}

		c.Children = newChildren

		// Init expected Targets to include existing initial targets that are still
		// in the updated children.
		expectedTargets = []string{}
		for _, chPath := range test.initChildren {
			if _, ok := test.updateChildren[chPath]; ok {
				expectedTargets = append(
					expectedTargets,
					fmt.Sprintf("%s.localhost:9101", chPath),
					fmt.Sprintf("%s.example.com:8888", chPath),
				)
			}
		}

		for chPath := range test.updateChildren {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.hostlocal.example.com:9101
        - %s.sammyshark.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			// Only add child that did not exist before to expected targets to
			// stay consistent with zk behavior where same child child update do not
			// exist.
			if _, ok := tg.children[chPath]; !ok {
				expectedTargets = append(
					expectedTargets,
					fmt.Sprintf("%s.hostlocal.example.com:9101", chPath),
					fmt.Sprintf("%s.sammyshark.example.com:8888", chPath),
				)
			}
		}
		// Send job update event and validate new results.
		c.SendChildrenEvent(1, zkRoot)

		// TODO address this temp hack below.  When deleting all children, we get a
		// unexpected  target slice of previous values of children before the expected
		// empty slice.
		go func() {
			for t := range tg.Targets() {
				targets = t
			}
		}()

		<-time.After(1 * time.Second)
		//**

		validateResults(targets, expectedTargets)

		tg.Stop()
	}

	var jobUpdateByCountTest = []struct {
		desc        string
		children    []string
		updateCount int
	}{
		{
			desc:        "1 update on 5 children",
			children:    []string{"child-a", "child-b", "child-c", "child-d", "child-e"},
			updateCount: 1,
		},
		{
			desc:        "5 update on 5 children",
			children:    []string{"child-a", "child-b", "child-c", "child-d", "child-e"},
			updateCount: 5,
		},
		{
			desc: "5 update on 26 children",
			children: []string{"child-a", "child-b", "child-c", "child-d", "child-f",
				"child-g", "child-h", "child-i", "child-j", "child-k", "child-l", "child-m",
				"child-n", "child-o", "child-p", "child-q", "child-r", "child-s", "child-t",
				"child-u", "child-v", "child-w", "child-x", "child-y", "child-z"},
			updateCount: 5,
		},
		{
			desc: "13 update on 26 children",
			children: []string{"child-a", "child-b", "child-c", "child-d", "child-f",
				"child-g", "child-h", "child-i", "child-j", "child-k", "child-l", "child-m",
				"child-n", "child-o", "child-p", "child-q", "child-r", "child-s", "child-t",
				"child-u", "child-v", "child-w", "child-x", "child-y", "child-z"},
			updateCount: 5,
		},
		{
			desc: "26 update on 26 children",
			children: []string{"child-a", "child-b", "child-c", "child-d", "child-e", "child-f",
				"child-g", "child-h", "child-i", "child-j", "child-k", "child-l", "child-m",
				"child-n", "child-o", "child-p", "child-q", "child-r", "child-s", "child-t",
				"child-u", "child-v", "child-w", "child-x", "child-y", "child-z"},
			updateCount: 26,
		},
	}

	for i, test := range jobUpdateByCountTest {
		t.Logf("job update by count test %d: %q", i, test.desc)

		zkRoot := "/vulca/test/targeter"
		c := NewZKConn()
		c.GetEventChannel = make(chan zk.Event)
		c.Children = test.children
		c.Jobs = make(map[string]string, len(test.children))
		expectedTargets := []string{}

		for _, chPath := range test.children {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.localhost:9101
        - %s.old.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.localhost:9101", chPath),
				fmt.Sprintf("%s.old.example.com:8888", chPath),
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: map[string]*chState{},
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    new(sync.Mutex),
			done:     make(chan struct{}),
		}

		go func() {
			tg.run()
		}()

		// Get initial targets
		targets := <-tg.Targets()
		validateResults(targets, expectedTargets)

		// Set up new job fetches
		for _, chPath := range test.children {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.hostlocal:9101
        - %s.sammyshark.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)
		}

		// Send job update event
		for i := 0; i < test.updateCount; i++ {
			c.SendGetEvent(1, zkRoot)
			targets = <-tg.Targets()
		}

		if len(targets) != len(expectedTargets) {
			t.Fatalf(
				"run() => expected %d targets after job update; got %d",
				len(targets),
				len(expectedTargets),
			)
		}

		// Count the number of init target key types and update target key types
		var (
			gotNew      int
			gotOld      int
			expectedNew = test.updateCount * 2
			expectedOld = len(expectedTargets) - (test.updateCount * 2)
		)

		for _, tgt := range targets {
			if strings.Contains(tgt.Key(), "localhost:9101") ||
				strings.Contains(tgt.Key(), "old.example.com:8888") {
				gotOld++
			}

			if strings.Contains(tgt.Key(), "hostlocal:9101") ||
				strings.Contains(tgt.Key(), "sammyshark.example.com:8888") {
				gotNew++
			}
		}

		if gotNew != expectedNew || gotOld != expectedOld {
			t.Errorf(
				"run() { %d updates } => got %d updated target keys and %d initial target keys; expected %d and %d",
				test.updateCount,
				gotNew,
				gotOld,
				expectedNew,
				expectedOld,
			)
		}
		tg.Stop()
	}
}

func TestSetChildren(t *testing.T) {

	var happPathTests = []struct {
		desc             string
		param            []string
		existingChildren []string
		expectedChildren map[string]struct{}
	}{
		{
			desc:  "no existing children",
			param: []string{"child-a", "child-b", "child-c"},
			expectedChildren: map[string]struct{}{
				"child-a": struct{}{}, "child-b": struct{}{}, "child-c": struct{}{},
			},
		},
		{
			desc:             "no new children",
			param:            []string{},
			existingChildren: []string{"child-a", "child-b", "child-c"},
			expectedChildren: map[string]struct{}{},
		},
		{
			desc:             "some new children",
			param:            []string{"child-a", "child-b", "child-c"},
			existingChildren: []string{"child-d", "child-e", "child-f"},
			expectedChildren: map[string]struct{}{
				"child-a": struct{}{}, "child-b": struct{}{}, "child-c": struct{}{},
			},
		},
	}

	for i, test := range happPathTests {
		t.Logf("happy path test %d: %q", i, test.desc)

		zkRoot := "/vulca/test/targeter"
		c := NewZKConn()
		c.GetEventChannel = make(chan zk.Event)
		c.Children = test.param

		c.Jobs = make(map[string]string, len(test.existingChildren)+len(test.param))
		for _, chPath := range append(test.existingChildren, test.param...) {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.localhost:9101
        - %s.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: mapChildren(test.existingChildren, c.Mock, zkRoot),
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    new(sync.Mutex),
		}

		testCh := make(chan struct{})

		go func() {
			go func() {
				for _ = range tg.Targets() {
				}
			}()

			tg.setChildren(test.param)
			testCh <- struct{}{}
		}()

		select {
		case <-time.After(3 * time.Second):
			t.Errorf(
				"setChildren(%v) => exceeded 3 second timelimit for test run",
				test.param,
			)
		case <-testCh:
			if len(tg.children) != len(test.expectedChildren) {
				t.Errorf(
					"setChildren(%v) => got %d children; expected %d",
					test.param,
					len(tg.children),
					len(test.expectedChildren),
				)
			}

			for child, state := range tg.children {
				if _, ok := test.expectedChildren[child]; !ok {
					t.Errorf(
						"setChildren(%v) => got child %s in current children: expected %v",
						test.param,
						child,
						test.expectedChildren,
					)
				}

				if len(state.targets) != 2 {
					t.Errorf(
						"setChildren(%v) => child %q with %d targets; expected 2",
						test.param,
						child,
						len(state.targets),
					)
				}

				for _, tgt := range state.targets {
					childName := strings.Split(child, "/")
					if strings.Count(tgt.Key(), childName[len(childName)-1]) != 2 {
						t.Errorf(
							"setChildren(%v) => child %q target key: %q; expected something else with %q",
							test.param,
							childName,
							tgt.Key(),
							childName,
						)
					}
				}
			}
		}
	}

	var existingChildrenTest = []struct {
		desc             string
		param1           []string
		param2           []string
		expectedChildren map[string]struct{}
	}{
		{
			desc:   "new children == old children",
			param1: []string{"child-a", "child-b", "child-c"},
			param2: []string{"child-a", "child-b", "child-c"},
			expectedChildren: map[string]struct{}{
				"child-a": struct{}{}, "child-b": struct{}{}, "child-c": struct{}{},
			},
		},
	}

	for i, test := range existingChildrenTest {
		t.Logf("existing children test %d: %q", i, test.desc)

		zkRoot := "/vulca/test/targeter"
		c := NewZKConn()
		c.GetEventChannel = make(chan zk.Event)

		c.Jobs = map[string]string{}
		for _, chPath := range append(test.param1, test.param2...) {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.localhost:9101
        - %s.example.com:8888
`,
				chPath,
				chPath,
				chPath,
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: map[string]*chState{},
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    new(sync.Mutex),
		}

		testCh := make(chan struct{})

		go func() {
			go func() {
				for _ = range tg.Targets() {
				}
			}()

			tg.setChildren(test.param1)
			tg.setChildren(test.param2)
			testCh <- struct{}{}
		}()

		select {
		case <-time.After(3 * time.Second):
			t.Errorf(
				"setChildren(%v) + setChildren(%v) => exceeded 3 second timelimit for test run",
				test.param1,
				test.param2,
			)
		case <-testCh:
			if len(tg.children) != len(test.expectedChildren) {
				t.Errorf(
					"setChildren(%v) + setChildren(%v) => got %d children; expected %d",
					test.param1,
					test.param2,
					len(tg.children),
					len(test.expectedChildren),
				)
			}

			for child, state := range tg.children {
				if _, ok := test.expectedChildren[child]; !ok {
					t.Errorf(
						"setChildren(%v) + setChildren(%v) => got child %s in current children: expected %v",
						test.param1,
						test.param2,
						child,
						test.expectedChildren,
					)
				}

				if len(state.targets) != 2 {
					t.Errorf(
						"setChildren(%v) + setChildren(%v) => child %q with %d targets; expected 2",
						test.param1,
						test.param2,
						child,
						len(state.targets),
					)
				}

				for _, tgt := range state.targets {
					childName := strings.Split(child, "/")
					if strings.Count(tgt.Key(), childName[len(childName)-1]) != 2 {
						t.Errorf(
							"setChildren(%v) + setChildren(%v) => child %q target key: %q; expected something else with %q",
							test.param1,
							test.param2,
							childName,
							tgt.Key(),
							childName,
						)
					}
				}
			}
		}
	}
}
