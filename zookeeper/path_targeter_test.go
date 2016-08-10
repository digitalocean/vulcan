package zookeeper

import (
	"errors"
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

		c := NewZKConn()
		c.EventChannel = make(chan zk.Event)
		c.Jobs = test.jobs

		pt := &PathTargeter{
			conn: c.Mock,
			path: "/vulcan/test/",
			done: make(chan struct{}),
			out:  make(chan scraper.Job),
		}

		testCh := make(chan struct{})

		go func() {
			go func() {
				for _ = range pt.Targets() {
				}
			}()

			pt.run()
			testCh <- struct{}{}
		}()

		if test.eventDelay > 0 {
			c.SendEvent(time.Duration(test.eventDelay) * time.Second)
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
      job_name: haproxy_stats
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
      job_name: haproxy_stats
      metrics_path: /metrics
      static_configs:
        - targets:
          - localhost:9101
          - localhost:9102
          - www.sammy.com:9101
          - net.droplet.org:8080
          - cool.storage.io:8888
  `,
			jobsByName: []string{"haproxy_stats"},
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
			for _, target := range test.targetsByName {
				if _, ok := job.Targets[scraper.Instance(target)]; !ok {
					t.Errorf(
						"parseJobs(%s) => jobs -> %v; expected target for %s",
						test.param,
						job.Targets,
						target,
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
