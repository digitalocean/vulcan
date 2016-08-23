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
	"github.com/serialx/hashring"
)

// ConsistentHashTargeter represents an object that orchestrates work between
// Zookeeper targets and available worker nodes in the pool.
type ConsistentHashTargeter struct {
	id         string
	pool       <-chan []string
	targets    <-chan []Targeter
	out        chan []Targeter
	mu         *sync.Mutex
	curPool    []string
	curTargets []Targeter
}

// NewConsistentHashTargeter returns a new instance of a ConsistentHashTargeter
// object.
func NewConsistentHashTargeter(config *ConsistentHashTargeterConfig) *ConsistentHashTargeter {
	cht := &ConsistentHashTargeter{
		id:         config.ID,
		pool:       config.Pool.Scrapers(),
		targets:    config.Targeter.Targets(),
		out:        make(chan []Targeter),
		curPool:    []string{},
		curTargets: []Targeter{},
		mu:         new(sync.Mutex),
	}

	go cht.run()

	return cht
}

// ConsistentHashTargeterConfig represents an configuration for a
// ConsistentHashTargeter object.
type ConsistentHashTargeterConfig struct {
	Targeter TargetWatcher
	ID       string
	Pool     Pool
}

// Targets returns a channel that feeds current available jobs.
func (cht *ConsistentHashTargeter) Targets() <-chan []Targeter {
	return cht.out
}

func (cht *ConsistentHashTargeter) run() {
	ll := log.WithFields(log.Fields{
		"consistent_hash_targter": "run",
		"uuid": cht.id,
	})
	ll.Debug("waiting for initial pool")
	cht.curPool = <-cht.pool
	ll.Debug("pool initialized")

	for {
		select {

		case nextPool := <-cht.pool:
			ll.WithFields(log.Fields{
				"current_targets": cht.curTargets,
				"new_pool":        nextPool,
			}).Debug("pool update received")

			cht.mu.Lock()

			cht.curPool = nextPool
			hashedTargets := cht.hashTargets(cht.curTargets)
			cht.out <- hashedTargets

			cht.mu.Unlock()
			ll.WithFields(log.Fields{
				"current_pool":    cht.curPool,
				"current_targets": hashedTargets,
			}).Debug("targets updated;")

		case nextTargets := <-cht.targets:
			ll.WithFields(log.Fields{
				"current_targets": cht.curTargets,
				"current_pool":    cht.curPool,
				"new_targets":     nextTargets,
			}).Debug("target update received")

			cht.mu.Lock()

			hashedTargets := cht.hashTargets(nextTargets)
			cht.out <- hashedTargets

			cht.mu.Unlock()
			ll.WithField("current_targets", hashedTargets).Debug("targets updated")
		}
	}
}

func (cht *ConsistentHashTargeter) hashTargets(targets []Targeter) []Targeter {
	var (
		result []Targeter
		ring   = hashring.New(cht.curPool)
	)

	for _, target := range targets {
		if id, _ := ring.GetNode(target.Key()); id == cht.id {
			result = append(result, target)
		}
	}

	cht.curTargets = targets

	return result
}
