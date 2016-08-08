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
					key: "foobar.digitalocean.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "barfoo.digitalocean.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "raboof.digitalocean.com",
				},
			},
		},
		{
			desc: "no matching running jobs",
			runningJobs: map[string]*Worker{
				"barfoo.digitalocean.com": &Worker{
					key: "barfoo.digitalocean.com",
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
					key: "foobar.digitalocean.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "raboof.digitalocean.com",
				},
			},
		},
		{
			desc: "all matching running jobs",
			runningJobs: map[string]*Worker{
				"foobar.digitalocean.com": &Worker{
					key: "foobar.digitalocean.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "foobar.digitalocean.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
			},
			param: []Targeter{
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "foobar.digitalocean.com",
				},
			},
		},
		{
			desc: "some matching running jobs",
			runningJobs: map[string]*Worker{
				"foobar.digitalocean.com": &Worker{
					key: "foobar.digitalocean.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "foobar.digitalocean.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
				"barfoo.digitalocean.com": &Worker{
					key: "barfoo.digitalocean.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "barfoo.digitalocean.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
				"baroof.digitalocean.com": &Worker{
					key: "baroof.digitalocean.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "baroof.digitalocean.com",
					},
					writer: &MockWriter{},
					done:   make(chan struct{}),
				},
			},
			param: []Targeter{
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "foobar.digitalocean.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "oofrab.digitalocean.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "raboof.digitalocean.com",
				},
				&MockTargeter{
					i:   time.Duration(30) * time.Second,
					key: "baroof.digitalocean.com",
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
				"barfoo.digitalocean.com": &Worker{
					key: "barfoo.digitalocean.com",
					Target: &MockTargeter{
						i:   time.Duration(30) * time.Second,
						key: "barfoo.digitalocean.com",
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
