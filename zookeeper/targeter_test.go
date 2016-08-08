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
				conn: conn,
				path: path.Join(rootPath, c),
				done: make(chan struct{}),
				out:  make(chan []scraper.Job),
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
				//fmt.Println("TARGET KEY IS ", got.Key())
				if strings.Contains(got.Key(), expected) {
					found = true
				}
			}

			if !found {
				t.Errorf(
					"run() => received targets did contain expected; expected: %q got: %v",
					expected,
					targets,
				)
			}
		}
	}

	var jobUpdateTest = []struct {
		desc            string
		children        []string
		expectedTargets map[string]struct{}
	}{
		{
			desc:     "3 children receives job event",
			children: []string{"child-a", "child-b", "child-c"},
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
        - %s.digitalocean.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.localhost:9101", chPath),
				fmt.Sprintf("%s.digitalocean.com:8888", chPath),
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: map[string]*chState{},
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    &sync.Mutex{},
		}

		close := make(chan struct{})
		go func() {
			tg.run()
			<-close
			return
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
        - %s.sammyshark.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.hostlocal:9101", chPath),
				fmt.Sprintf("%s.sammyshark.com:8888", chPath),
			)
		}
		// Send job update event

		for i := 0; i < len(test.children); i++ {
			c.SendGetEvent(1, zkRoot)
			targets = <-tg.Targets()
		}
		validateResults(targets, expectedTargets)
	}

	var childrenUpdateTest = []struct {
		desc           string
		initChildren   []string
		updateChildren []string
	}{
		{
			desc:           "all new children",
			initChildren:   []string{"child-a", "child-b", "child-c"},
			updateChildren: []string{"child-d", "child-e", "child-f"},
		},
	}

	for i, test := range childrenUpdateTest {
		t.Logf("children update test %d: %q", i, test.desc)

		zkRoot := "/vulca/test/targeter"
		c := NewZKConn()
		c.ChildrenEventChannel = make(chan zk.Event)
		c.Children = test.initChildren
		c.Jobs = make(map[string]string, len(test.initChildren))
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
        - %s.digitalocean.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.localhost:9101", chPath),
				fmt.Sprintf("%s.digitalocean.com:8888", chPath),
			)
		}

		tg := Targeter{
			conn:     c.Mock,
			children: map[string]*chState{},
			path:     zkRoot,
			out:      make(chan []scraper.Targeter),
			mutex:    &sync.Mutex{},
		}

		close := make(chan struct{})
		go func() {
			tg.run()
			<-close
			return
		}()

		// Get initial targets
		targets := <-tg.Targets()
		validateResults(targets, expectedTargets)

		// Set up new targets
		c.Children = test.updateChildren
		expectedTargets = []string{}
		for _, chPath := range test.updateChildren {
			c.Jobs[path.Join(zkRoot, chPath)] = fmt.Sprintf(`
scrape_configs:
  -
    job_name: haproxy_stats-%s
    metrics_path: /metrics
    static_configs:
      - targets:
        - %s.hostlocal:9101
        - %s.sammyshark.com:8888
`,
				chPath,
				chPath,
				chPath,
			)

			expectedTargets = append(
				expectedTargets,
				fmt.Sprintf("%s.hostlocal:9101", chPath),
				fmt.Sprintf("%s.sammyshark.com:8888", chPath),
			)
		}
		// Send job update event and validate new results
		c.SendChildrenEvent(1, zkRoot)
		targets = <-tg.Targets()
		validateResults(targets, expectedTargets)
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
        - %s.digitalocean.com:8888
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
        - %s.digitalocean.com:8888
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
