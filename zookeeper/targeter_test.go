package zookeeper

import (
	"testing"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	"github.com/samuel/go-zookeeper/zk"
)

func mapChildren(cn []string, conn Client) map[string]*PathTargeter {
	m := make(map[string]*PathTargeter, len(cn))

	for _, c := range cn {
		m[c] = &PathTargeter{
			conn: conn,
			path: "/vulcan/test",
			done: make(chan struct{}),
			out:  make(chan scraper.Job),
		}
		go m[c].run()
	}

	return m
}

func TestTargeterRun(t *testing.T) {

}

func TestSetChildren(t *testing.T) {
	var happPathTests = []struct {
		desc             string
		param            []string
		existingChildren []string
		expectedChildren map[string]struct{}
	}{
		{
			desc:             "new children == old children",
			param:            []string{"child-a", "child-b", "child-c"},
			existingChildren: []string{"child-a", "child-b", "child-c"},
			expectedChildren: map[string]struct{}{
				"child-a": struct{}{}, "child-b": struct{}{}, "child-c": struct{}{},
			},
		},
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

		c := &ZKConn{
			EventChannel: make(chan zk.Event),
			Children:     test.param,
			Jobs: `
scrape_configs:
  -
    job_name: haproxy_stats
    metrics_path: /metrics
    static_configs:
      - targets:
        - localhost:9101
`,
		}

		tg := Targeter{
			conn:     c,
			children: mapChildren(test.existingChildren, c),
			path:     "/vulca/test/targeter",
			out:      make(chan scraper.Job),
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

			for child := range tg.children {
				if _, ok := test.expectedChildren[child]; !ok {
					t.Errorf(
						"setChildren(%v) => got child %s in current children: expected %v",
						test.param,
						child,
						test.expectedChildren,
					)
				}
			}
		}
	}
}
