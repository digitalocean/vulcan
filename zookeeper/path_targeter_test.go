package zookeeper

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	"github.com/samuel/go-zookeeper/zk"
)

func TestPathTargeterRun(t *testing.T) {
	var runValidations = []struct {
		desc string
		// Need to decide if it's worthwhile to validate all jobs coming out of
		// the out channel.  For now, jobs and numOfJobs are not used in any test
		// validation steps.
		jobs       string
		numOfJobs  int
		eventDelay int
		closeDelay int
		connGetErr error
	}{
		{
			desc: "one job, event before close",
			jobs: `
scrape_configs:
  -
    job_name: haproxy_stats
    metrics_path: /metrics
    static_configs:
      - targets:
        - localhost:9101
`,
			numOfJobs:  1,
			eventDelay: 1,
			closeDelay: 5,
		},
		{
			desc: "one job, close before event",
			jobs: `
scrape_configs:
  -
    job_name: haproxy_stats
    metrics_path: /metrics
    static_configs:
      - targets:
        - localhost:9101
`,
			numOfJobs:  1,
			eventDelay: 7,
			closeDelay: 5,
		},
		{
			desc: "constant zk connection errors",
			jobs: `
scrape_configs:
  -
    job_name: haproxy_stats
    metrics_path: /metrics
    static_configs:
      - targets:
        - localhost:9101
`,
			numOfJobs:  1,
			closeDelay: 5,
			connGetErr: errors.New(GetErrMsg),
		},
	}

	for i, test := range runValidations {
		t.Logf("run validations %d: %q", i, test.desc)

		testPath := "/vulcan/test/"
		c := NewZKConn()
		c.GetEventChannel = make(chan zk.Event)
		c.Jobs = map[string]string{testPath: test.jobs}

		pt := &PathTargeter{
			conn: c.Mock,
			path: testPath,
			done: make(chan struct{}),
			out:  make(chan []scraper.Job),
		}

		testCh := make(chan struct{})

		go func() {
			go func() {
				for _ = range pt.Jobs() {
				}
			}()

			pt.run()
			testCh <- struct{}{}
		}()

		if test.eventDelay > 0 {
			c.SendGetEvent(time.Duration(test.eventDelay)*time.Second, testPath)
		}

		go func() {
			time.Sleep(time.Duration(test.closeDelay) * time.Second)
			pt.stop()
		}()

		select {
		case <-time.After(time.Duration(test.closeDelay+3) * time.Second):
			t.Errorf(
				"run() => expected but close within %d seconds but time exceeded",
				test.closeDelay+1,
			)
		case <-testCh:
			t.Logf("happy path test %d successful", i)
		}
	}
}

func TestParseJobs(t *testing.T) {
	var happyPathTests = []struct {
		desc          string
		param         string
		jobsByName    []string
		targetsByName []string
	}{
		{
			desc: "1 job 1 target",
			param: `
  scrape_configs:
    -
      job_name: static_configs
      metrics_path: /metrics
      static_configs:
        - targets:
          - localhost:9101
  `,
			jobsByName:    []string{"haproxy_stats"},
			targetsByName: []string{"localhost:9101"},
		},
		{
			desc: "1 job 5 target",
			param: `
  scrape_configs:
    -
      job_name: static_configs
      metrics_path: /metrics
      static_configs:
        - targets:
          - localhost:9101
          - localhost:9102
          - www.sammy.com:9101
          - net.droplet.org:8080
          - cool.storage.io:8888
  `,
			jobsByName: []string{"static_configs"},
			targetsByName: []string{"localhost:9101", "localhost:9102",
				"www.sammy.com:9101", "net.droplet.org:8080", "cool.storage.io:8888"},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path test %d: %q", i, test.desc)

		pt := &PathTargeter{
			conn: NewZKConn().Mock,
			path: "/vulcan/test",
		}

		jobs, err := pt.parseJobs([]byte(test.param))
		if err != nil {
			t.Errorf(
				"parseJobs(%s) => %v,%v; expected nil errors",
				test.param,
				jobs,
				err,
			)
		}

		if len(jobs) != len(test.jobsByName) {
			t.Errorf(
				"parseJobs(%s) => %v == %d jobs; expected %d;",
				test.param,
				jobs,
				len(jobs),
				len(test.jobsByName),
			)
		}

		for _, job := range jobs {

			for _, expectedTarget := range test.targetsByName {
				var found bool
				for _, gotTarget := range job.GetTargets() {
					if strings.Contains(gotTarget.Key(), expectedTarget) {
						found = true
					}
				}
				if !found {
					t.Errorf(
						"parseJobs(%s) => jobs -> %v; expected target for %s",
						test.param,
						job.GetTargets(),
						expectedTarget,
					)
				}
			}
		}
	}

	var negativeTests = []struct {
		desc  string
		param string
	}{
		{
			desc: "bad yaml format",
			param: `
scrape_configs:
  -
    job_name: haproxy_stats
    metrics_path: /metrics
    static_targets:
      - targets:
        - localhost:9101
`,
		},
		{
			desc: "zero scrape_configs",
			param: `
scrape_configs:
`,
		},
		{
			desc: "zero length byte slice",
		},
		// 		{
		// 			desc: "bad url format",
		// 			param: `
		// scrape_configs:
		//   -
		//     job_name: haproxy_stats
		//     scheme: √©≥÷µ
		//     metrics_path: √fhj//&%&\\|\©≥÷µ
		//     static_configs:
		//       - targets:
		//         - localhost:9101
		// `,
		// 		},
	}
	for i, test := range negativeTests {
		t.Logf("negative test %d: %q", i, test.desc)

		pt := &PathTargeter{
			conn: NewZKConn().Mock,
			path: "/vulcan/test",
		}

		if _, err := pt.parseJobs([]byte(test.param)); err == nil {
			t.Errorf(
				"parseJobs(%s) => _, nil; expected an error",
				test.param,
			)
		}
	}
}
