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
	"sync"

	log "github.com/Sirupsen/logrus"
)

// Scraper answers to a query on which targets are running and groups them
// by job.
type Scraper struct {
	Targeter TargetWatcher
	Writer   Writer
	running  map[string]*Worker
	done     chan struct{}
	once     sync.Once
}

// Config represents an instance of a scraper configuration.
// It contains a Targeter interface and a Writer interface.
type Config struct {
	Targeter TargetWatcher
	Writer   Writer
}

// NewScraper returns a new Scraper instance from the provided Config.
func NewScraper(config *Config) *Scraper {
	return &Scraper{
		Targeter: config.Targeter,
		Writer:   config.Writer,
		running:  map[string]*Worker{},
		done:     make(chan struct{}),
	}
}

// Run ranges over the Scraper's targets and does work on them.
func (s *Scraper) Run() {
	for {
		select {
		case target := <-s.Targeter.Targets():
			s.set(target)

			log.WithFields(log.Fields{
				"scraper":      "run",
				"target_count": len(target),
			}).Info("scraping targets")

		case <-s.done:
			return

		default:
		}
	}
}

func (s *Scraper) set(targets []Targeter) {
	next := make(map[string]*Worker, len(targets))

	for _, target := range targets {
		worker, ok := s.running[target.Key()]
		if !ok {
			next[target.Key()] = NewWorker(&WorkerConfig{
				Key:    target.Key(),
				Target: target,
				Writer: s.Writer,
			})
			continue
		}

		if !worker.Target.Equals(target) {
			worker.Retarget(target)
		}
		// Set found worker to next map of workers and remove reference from
		// existing running.
		next[target.Key()] = worker
		delete(s.running, target.Key())
	}
	// Stop runnning jobs that are no longer active targets targets.
	for _, w := range s.running {
		w.Stop()
	}

	s.running = next
}

// Stop signals the Scraper instance to stop running.
func (s *Scraper) Stop() {
	s.once.Do(func() {
		close(s.done)
	})
}
