package zookeeper

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/digitalocean/vulcan/scraper"

	"github.com/prometheus/common/model"
	pconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/samuel/go-zookeeper/zk"
)

// TODO impement JobDeepEquals

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
			conn:  c.Mock,
			path:  testPath,
			done:  make(chan struct{}),
			out:   make(chan []scraper.Job),
			mutex: new(sync.Mutex),
			jobs:  map[string]scraper.Job{},
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

func TestK8Jobs(t *testing.T) {
	initialize := func(delay time.Duration) (
		pt *PathTargeter,
		tp *MockTargetProvider,
		ctxCancelFunc func(),
	) {
		sc := &pconfig.ScrapeConfig{
			JobName: "test",
			KubernetesSDConfigs: []*pconfig.KubernetesSDConfig{
				&pconfig.KubernetesSDConfig{
					APIServers: []pconfig.URL{},
				},
			},
			Scheme:      "http",
			MetricsPath: "/metrics",
		}

		pt = &PathTargeter{
			conn:  NewZKConn().Mock,
			path:  "/sammy/test",
			done:  make(chan struct{}),
			out:   make(chan []scraper.Job),
			mutex: new(sync.Mutex),
			jobs:  map[string]scraper.Job{},
		}

		// set up mock target provider
		tp = &MockTargetProvider{Interval: delay}
		providerFn := func(c *pconfig.KubernetesSDConfig) (retrieval.TargetProvider, error) {
			return tp, nil
		}

		ctx, cancelFunc := context.WithCancel(context.Background())

		go pt.k8Jobs(sc, providerFn, ctx)

		return pt, tp, cancelFunc
	}
	// We expected always n-1 updates on the out channel b/c the first update
	// relies on the run() function to send.
	var moreThanOneEventTests = []struct {
		desc            string
		delay           time.Duration
		expectedUpdates int
	}{
		{
			desc:            "2 target provider channel events, 1 update",
			delay:           time.Duration(500 * time.Millisecond),
			expectedUpdates: 1,
		},
		{
			desc:            "3 target provider channel events, 2 updates",
			delay:           time.Duration(2 * time.Second),
			expectedUpdates: 2,
		},
		{
			desc:            "20 target provider channel events, 19 updates",
			delay:           time.Duration(250 * time.Millisecond),
			expectedUpdates: 19,
		},
		{
			desc:            "100 target provider channel events, 99 updates",
			delay:           time.Duration(50 * time.Millisecond),
			expectedUpdates: 99,
		},
	}

	for i, test := range moreThanOneEventTests {
		t.Logf("more than 1 event tests %d: %q", i, test.desc)

		pt, tp, cancelFunc := initialize(test.delay)

		// set up counter for update test validation
		var updateCount int

		go func() {
			for _ = range pt.Jobs() {
				updateCount++
			}
		}()

		// send out events thru mock target provider
		go func() {
			for i := 0; i <= test.expectedUpdates; i++ {
				tp.SendTargetGroups([]*pconfig.TargetGroup{
					&pconfig.TargetGroup{
						Source: fmt.Sprintf("test-%d", i),
						Targets: []model.LabelSet{
							model.LabelSet{
								"__address__": "foobar.example.com",
							},
							model.LabelSet{
								"__address__": "barfoo.example.com",
							},
						},
					},
				})
			}
		}()

		// give enougth time for events to process
		time.Sleep(test.delay * time.Duration(2*(test.expectedUpdates+1)))

		cancelFunc()

		if updateCount != test.expectedUpdates {
			t.Fatalf(
				"k8Jobs() => %d update events; expected %d",
				updateCount,
				test.expectedUpdates,
			)
		}

		// validate current jobs of part-targeter instance
		for i := 0; i <= test.expectedUpdates; i++ {
			key := fmt.Sprintf("%s/0/test-%d", kubernetesJob, i)
			if _, ok := pt.jobs[key]; !ok {
				t.Errorf(
					"k8Jobs() => expected job key %q in current jobs; found none",
					key,
				)
			}
		}
	}

	var initialEventTest = []struct {
		desc  string
		delay time.Duration
	}{
		{
			desc:  "1 event, 1 second delay",
			delay: time.Duration(1 * time.Second),
		},
	}

	for i, test := range initialEventTest {
		t.Logf("initial event test %d: %q", i, test.desc)

		pt, tp, cancelFunc := initialize(test.delay)

		go func() {
			for _ = range pt.Jobs() {
			}
		}()

		go func() {
			tp.SendTargetGroups([]*pconfig.TargetGroup{
				&pconfig.TargetGroup{
					Source: fmt.Sprintf("test-0"),
					Targets: []model.LabelSet{
						model.LabelSet{
							"__address__": "foobar.example.com",
						},
						model.LabelSet{
							"__address__": "barfoo.example.com",
						},
					},
				},
			})
		}()

		// sleep to get event time to finish
		time.Sleep(2 * test.delay)

		cancelFunc()

		key := fmt.Sprintf("%s/0/test-0", kubernetesJob)
		if _, ok := pt.jobs[key]; !ok {
			t.Errorf(
				"k8Jobs() => expected job key %q in current jobs; found none",
				key,
			)
		}
	}
}

func TestTgToJob(t *testing.T) {
	var happyPathTests = []struct {
		desc                 string
		tg                   *pconfig.TargetGroup
		sc                   *pconfig.ScrapeConfig
		expectedTargeterKeys []string
	}{
		{
			desc: "1 target",
			tg: &pconfig.TargetGroup{
				Source: "test",
				Targets: []model.LabelSet{
					model.LabelSet{
						"__address__": "foobar.example.com",
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:        "test1",
				Scheme:         "http",
				ScrapeInterval: model.Duration(15),
				MetricsPath:    "/metrics",
			},
			expectedTargeterKeys: []string{
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar.example.com", Path: "/metrics", Scheme: "http"}),
			},
		},
		{
			desc: "2 targets",
			tg: &pconfig.TargetGroup{
				Source: "test",
				Targets: []model.LabelSet{
					model.LabelSet{
						"__address__": "foobar.example.com",
					},
					model.LabelSet{
						"__address__": "barfoo.example.com",
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:        "test1",
				Scheme:         "http",
				ScrapeInterval: model.Duration(15),
				MetricsPath:    "/metrics",
			},
			expectedTargeterKeys: []string{
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "barfoo.example.com", Path: "/metrics", Scheme: "http"}),
			},
		},
		{
			desc: "10 targets",
			tg: &pconfig.TargetGroup{
				Source: "test",
				Targets: []model.LabelSet{
					model.LabelSet{
						"__address__": "foobar0.example.com",
					},
					model.LabelSet{
						"__address__": "foobar1.example.com",
					},
					model.LabelSet{
						"__address__": "foobar2.example.com",
					},
					model.LabelSet{
						"__address__": "foobar3.example.com",
					},
					model.LabelSet{
						"__address__": "foobar4.example.com",
					},
					model.LabelSet{
						"__address__": "foobar5.example.com",
					},
					model.LabelSet{
						"__address__": "foobar6.example.com",
					},
					model.LabelSet{
						"__address__": "foobar7.example.com",
					},
					model.LabelSet{
						"__address__": "foobar8.example.com",
					},
					model.LabelSet{
						"__address__": "foobar9.example.com",
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:        "test1",
				Scheme:         "http",
				ScrapeInterval: model.Duration(15),
				MetricsPath:    "/metrics",
			},
			expectedTargeterKeys: []string{
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar0.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar1.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar2.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar3.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar4.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar5.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar6.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar7.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar8.example.com", Path: "/metrics", Scheme: "http"}),
				fmt.Sprintf("test1-%s", &url.URL{Host: "foobar9.example.com", Path: "/metrics", Scheme: "http"}),
			},
		},
		{
			desc: "10 targets, none valid",
			tg: &pconfig.TargetGroup{
				Source: "test",
				Targets: []model.LabelSet{
					model.LabelSet{
						"__a__": "foobar0.example.com",
					},
					model.LabelSet{
						"__a__": "foobar1.example.com",
					},
					model.LabelSet{
						"__a__": "foobar2.example.com",
					},
					model.LabelSet{
						"__a__": "foobar3.example.com",
					},
					model.LabelSet{
						"__a__": "foobar4.example.com",
					},
					model.LabelSet{
						"__a__": "foobar5.example.com",
					},
					model.LabelSet{
						"__a__": "foobar6.example.com",
					},
					model.LabelSet{
						"__a__": "foobar7.example.com",
					},
					model.LabelSet{
						"__a__": "foobar8.example.com",
					},
					model.LabelSet{
						"__a__": "foobar9.example.com",
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:        "test1",
				Scheme:         "http",
				ScrapeInterval: model.Duration(15),
				MetricsPath:    "/metrics",
			},
			expectedTargeterKeys: []string{},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path tests %d: %q", i, test.desc)

		pt := &PathTargeter{
			conn: NewZKConn().Mock,
		}

		j := pt.tgToJob(test.tg, test.sc)
		if len(j.GetTargets()) != len(test.expectedTargeterKeys) {
			t.Errorf(
				"tgToJob(%v, %v) => job with %d Targeters; got %d",
				*test.tg,
				*test.sc,
				len(j.GetTargets()),
				len(test.expectedTargeterKeys),
			)
		}

		for _, key := range test.expectedTargeterKeys {
			var found bool

			for _, targeter := range j.GetTargets() {
				if targeter.Key() == key {
					found = true
				}
			}

			if !found {
				t.Errorf(
					"tgToJob(%v, %v) => expected targeter key %q; found none",
					*test.tg,
					*test.sc,
					key,
				)
			}
		}
	}

}

// TODO Integrate JobDeepEquals for better test validation.
func TestAllJobs(t *testing.T) {
	var happyPathTests = []struct {
		desc        string
		currentJobs map[string]scraper.Job
		expected    []scraper.Job
	}{
		{
			desc: "1 job, 1 target",
			currentJobs: map[string]scraper.Job{
				"foo-static": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
			expected: []scraper.Job{
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},
		{
			desc: "1 job, 2 targets",
			currentJobs: map[string]scraper.Job{
				"foo-static": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
			expected: []scraper.Job{
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},
		{
			desc: "2 jobs, 1 target each",
			currentJobs: map[string]scraper.Job{
				"foo-static": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
			expected: []scraper.Job{
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},

		{
			desc: "10 jobs, 1 target each",
			currentJobs: map[string]scraper.Job{
				"foo-static": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-0": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo0.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-1": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo1.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-2": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo2.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-3": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo3.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-4": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo4.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-5": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo5.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-6": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo6.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-7": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo7.example.com", Path: "/metrics"},
						}),
					},
				}),
				"foo-k8-8": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo8.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
			expected: []scraper.Job{
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo0.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo1.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo2.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo3.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo4.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo5.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo6.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo7.example.com", Path: "/metrics"},
						}),
					},
				}),
				scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "foo",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "foo",
							URL:     &url.URL{Scheme: "http", Host: "barfoo8.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path test %d: %q", i, test.desc)

		pt := &PathTargeter{
			conn:  NewZKConn().Mock,
			done:  make(chan struct{}),
			out:   make(chan []scraper.Job),
			mutex: new(sync.Mutex),
			jobs:  test.currentJobs,
		}

		got := pt.allJobs()

		if len(got) != len(test.expected) {
			t.Errorf(
				"AllJobs() => got %d jobs; expected %d",
				len(got),
				len(test.expected),
			)
		}
	}

}

// TODO integrate JobDeepEquals for better test validation.
func TestSetJob(t *testing.T) {
	var happyPathTests = []struct {
		desc        string
		key         string
		tgs         []*pconfig.TargetGroup
		sc          *pconfig.ScrapeConfig
		currentJobs map[string]scraper.Job
		expected    map[string]scraper.Job
	}{
		{
			desc: "set 1 job, no currentJobs",
			key:  "test123-k8",
			tgs: []*pconfig.TargetGroup{
				&pconfig.TargetGroup{
					Source: "test",
					Targets: []model.LabelSet{
						model.LabelSet{
							"__address__": "foobar.example.com",
						},
						model.LabelSet{
							"__address__": "barfoo.example.com",
						},
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:     "testjob1",
				Scheme:      "http",
				MetricsPath: "/metrics",
			},
			currentJobs: map[string]scraper.Job{},
			expected: map[string]scraper.Job{
				"test123-k8/test": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},
		{
			desc: "set 1 job, target source is only current job",
			key:  "test123-k8",
			tgs: []*pconfig.TargetGroup{
				&pconfig.TargetGroup{
					Source: "test",
					Targets: []model.LabelSet{
						model.LabelSet{
							"__address__": "foobar.example.com",
						},
						model.LabelSet{
							"__address__": "barfoo.example.com",
						},
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:     "testjob1",
				Scheme:      "http",
				MetricsPath: "/metrics",
			},
			currentJobs: map[string]scraper.Job{
				"test123-k8/test": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
			expected: map[string]scraper.Job{
				"test123-k8/test": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},
		{
			desc: "set 2 target group, 1 key exists in current jobs",
			key:  "test123-k8",
			tgs: []*pconfig.TargetGroup{
				&pconfig.TargetGroup{
					Source: "test",
					Targets: []model.LabelSet{
						model.LabelSet{
							"__address__": "foobar.example.com",
						},
						model.LabelSet{
							"__address__": "barfoo.example.com",
						},
					},
				},
				&pconfig.TargetGroup{
					Source: "test2",
					Targets: []model.LabelSet{
						model.LabelSet{
							"__address__": "foobar.oceandigital.example.com",
						},
						model.LabelSet{
							"__address__": "barfoo.oceandigital.example.com",
						},
					},
				},
			},
			sc: &pconfig.ScrapeConfig{
				JobName:     "testjob1",
				Scheme:      "http",
				MetricsPath: "/metrics",
			},
			currentJobs: map[string]scraper.Job{
				"test123-k8/test": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
					},
				}),
				"test123-otherjob": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "sammy.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
			expected: map[string]scraper.Job{
				"test123-k8/test": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "foobar.example.com", Path: "/metrics"},
						}),
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.example.com", Path: "/metrics"},
						}),
					},
				}),
				"test123-k8/test2": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "foobar.oceandigital.example.com", Path: "/metrics"},
						}),
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "barfoo.oceandigital.example.com", Path: "/metrics"},
						}),
					},
				}),
				"test123-otherjob": scraper.NewStaticJob(&scraper.StaticJobConfig{
					JobName: "testjob1",
					Targeters: []scraper.Targeter{
						scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
							JobName: "testjob1",
							URL:     &url.URL{Scheme: "http", Host: "sammy.example.com", Path: "/metrics"},
						}),
					},
				}),
			},
		},
	}

	for i, test := range happyPathTests {
		t.Logf("happy path test %d: %q", i, test.desc)

		pt := &PathTargeter{
			conn:  NewZKConn().Mock,
			done:  make(chan struct{}),
			out:   make(chan []scraper.Job),
			mutex: new(sync.Mutex),
			jobs:  test.currentJobs,
		}

		pt.setJob(test.key, test.tgs, test.sc)

		for key, expectedTgt := range test.expected {
			gotTgt, ok := pt.jobs[key]
			if !ok {
				t.Fatalf(
					"setJob(%s, %v, %v) => expected job for key %q but not found",
					test.key,
					test.tgs,
					*test.sc,
					key,
				)
			}

			if len(gotTgt.GetTargets()) != len(expectedTgt.GetTargets()) {
				t.Errorf(
					"setJob(%s, %v, %v) => got %d targets for key %q; expected %d",
					test.key,
					test.tgs,
					*test.sc,
					len(gotTgt.GetTargets()),
					key,
					len(expectedTgt.GetTargets()),
				)
			}
		}
	}
}
