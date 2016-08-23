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
	"path"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

type chState struct {
	pt      *PathTargeter
	targets []scraper.Targeter
}

// Targeter uses zookeeper as a backend for configuring jobs that vulcan should
// scrape. The targeter watches the zookeeper path to react to new/changed/removed
// child nodes and their corresponding nodes.
type Targeter struct {
	conn     Client
	path     string
	children map[string]*chState

	out  chan []scraper.Targeter
	done chan struct{}

	once  sync.Once
	mutex *sync.Mutex
}

// NewTargeter returns a new instance of Targeter.
func NewTargeter(config *TargeterConfig) (*Targeter, error) {
	t := &Targeter{
		conn:     config.Conn,
		path:     path.Join(config.Root, "scraper", "jobs"),
		children: map[string]*chState{},
		out:      make(chan []scraper.Targeter),
		mutex:    new(sync.Mutex),
		done:     make(chan struct{}),
	}
	go t.run()
	return t, nil
}

// TargeterConfig represents the configuration of a Targeter.
type TargeterConfig struct {
	Conn Client
	Root string
}

// Targets implements scraper.Targeter interface.
// Returns a channel that feeds available jobs.
func (t *Targeter) Targets() <-chan []scraper.Targeter {
	return t.out
}

func (t *Targeter) run() {
	defer close(t.out)

	ll := log.WithFields(log.Fields{
		"path":     t.path,
		"targeter": "run",
	})

	for {
		select {
		case <-t.done:
			return

		default:
		}

		log.WithField("path", t.path).Info("reading child nodes from zookeeper")

		ech, err := t.updateChildren()
		if err != nil {
			ll.WithError(err).Error("unable to get list of jobs from zookeeper")

			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}

		done := make(chan struct{})
		for n, state := range t.children {
			go t.listenForJobs(n, state, done)
		}

		ll.WithField("num_jobs", len(t.children)).Info("set jobs list")

		select {
		case <-ech:
			ll.Debug("watch event received for ChildrenW")
			close(done)

		case <-t.done:
			close(done)
			return
		}
	}
}

func (t *Targeter) listenForJobs(childName string, state *chState, done <-chan struct{}) {
	ll := log.WithFields(log.Fields{
		"targeter": "listenForJobs",
		"child":    childName,
	})

	for {
		select {
		case jobs := <-state.pt.Jobs():

			ll.Debug("job update received")

			var targets []scraper.Targeter
			for _, job := range jobs {
				targets = append(targets, job.GetTargets()...)
			}
			t.mutex.Lock()

			t.children[childName].targets = targets
			t.out <- t.allTargets()

			t.mutex.Unlock()

		case <-done:
			return
		}
	}
}

func (t *Targeter) updateChildren() (<-chan zk.Event, error) {
	c, _, ech, err := t.conn.ChildrenW(t.path)
	if err != nil {
		return nil, errors.Wrap(err, "could not get children from zookeeper")
	}

	t.setChildren(c)
	t.out <- t.allTargets()
	return ech, nil
}

func (t *Targeter) setChildren(cn []string) {
	var (
		next = make(map[string]*chState, len(cn))
		wg   = new(sync.WaitGroup)
	)

	for _, c := range cn {
		if st, ok := t.children[c]; ok {
			next[c] = st
			delete(t.children, c)
			continue
		}

		wg.Add(1)

		p := path.Join(t.path, c)

		st := &chState{
			pt: NewPathTargeter(&PathTargeterConfig{
				Conn: t.conn,
				Path: p,
			}),
		}

		next[c] = st

		go func() {
			defer wg.Done()
			jobs := <-st.pt.Jobs()
			targets := []scraper.Targeter{}
			for _, j := range jobs {
				targets = append(targets, j.GetTargets()...)
			}
			st.targets = targets
		}()
	}

	wg.Wait()

	for _, st := range t.children {
		st.pt.stop()
	}

	t.mutex.Lock()
	t.children = next
	t.mutex.Unlock()
}

func (t *Targeter) allTargets() []scraper.Targeter {
	var results []scraper.Targeter

	for _, child := range t.children {
		results = append(results, child.targets...)
	}

	return results
}

// Stop signals the targeter instance to stop running.
func (t *Targeter) Stop() {
	t.once.Do(func() {
		close(t.done)
	})
}

func targetersFrJob(j scraper.Job) []scraper.Targeter {
	return nil
}
