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
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Worker represents an instance of a scraper worker.
type Worker struct {
	// jobName JobName
	key    string
	Target Targeter
	last   time.Time
	writer Writer
	done   chan struct{}
	once   sync.Once
}

// NewWorker creates a new instance of a Worker.
func NewWorker(config *WorkerConfig) *Worker {
	w := &Worker{
		key:    config.Key,
		Target: config.Target,
		writer: config.Writer,
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

// WorkerConfig respresents an instance of a Worker's configuration.
type WorkerConfig struct {
	// JobName JobName
	Key    string
	Target Targeter
	Writer Writer
}

func (w *Worker) run() {
	var (
		splay  = time.Duration(rand.Int63n(int64(w.Target.Interval()))) // TODO make this a consistent splay based off of metric name
		ticker = newSplayTicker(splay, w.Target.Interval())
		nowch  = ticker.C()
		ll     = log.WithField("worker", w.key)
	)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-nowch:
			ll.Debug("fetching target")
			fams, err := w.Target.Fetch()
			if err != nil {
				continue // keep trying
			}

			ll.Debug("writing metric")
			if err = w.writer.Write(w.key, fams); err != nil {
				ll.WithError(err).Error("first write failed. Retrying after 1s..")
				time.Sleep(1 * time.Second)

				if err = w.writer.Write(w.key, fams); err != nil {
					ll.WithError(err).Error("could not write metric")
				}
			}
		}
	}
}

// Retarget sets the current Worker's target to the parameter t.
func (w *Worker) Retarget(t Targeter) {
	w.Target = t
}

// Stop signals the current Worker instance to stop running.
func (w *Worker) Stop() {
	w.once.Do(func() {
		close(w.done)
	})
}
