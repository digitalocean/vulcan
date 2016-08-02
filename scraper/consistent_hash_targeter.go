package scraper

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/serialx/hashring"
)

// ConsistentHashTargeter represents an object that orchestrates work between
// Zookeeper targets and available worker nodes in the pool.
type ConsistentHashTargeter struct {
	id      string
	pool    <-chan []string
	jobs    <-chan Job
	out     chan Job
	mu      sync.Mutex
	curPool []string
	curJobs map[JobName]Job
}

// NewConsistentHashTargeter returns a new instance of a ConsistentHashTargeter
// object.
func NewConsistentHashTargeter(config *ConsistentHashTargeterConfig) *ConsistentHashTargeter {
	cht := &ConsistentHashTargeter{
		id:      config.ID,
		pool:    config.Pool.Scrapers(),
		jobs:    config.Targeter.Targets(),
		out:     make(chan Job),
		curPool: []string{},
		curJobs: map[JobName]Job{},
	}
	go cht.run()
	return cht
}

// ConsistentHashTargeterConfig represents an configuration for a
// ConsistentHashTargeter object.
type ConsistentHashTargeterConfig struct {
	Targeter Targeter
	ID       string
	Pool     Pool
}

// Targets returns a channel that feeds current available jobs.
func (cht *ConsistentHashTargeter) Targets() <-chan Job {
	return cht.out
}

func (cht *ConsistentHashTargeter) run() {
	for {
		select {
		case nextPool, ok := <-cht.pool:
			if !ok {
				log.Error("the pool is not alright!")
				return
			}
			cht.mu.Lock()
			cht.curPool = nextPool
			cht.rehashAll()
			cht.mu.Unlock()
		case nextJob := <-cht.jobs:
			cht.mu.Lock()
			cht.curJobs[nextJob.JobName] = nextJob
			cht.rehash(nextJob.JobName)
			cht.mu.Unlock()
		}
	}
}

func (cht *ConsistentHashTargeter) rehash(jobName JobName) {
	job := cht.curJobs[jobName]
	if len(job.Targets) == 0 {
		cht.out <- job
		delete(cht.curJobs, jobName)
		return
	}
	myTargets := cht.hashTargets(job.JobName, job.Targets)
	cht.out <- Job{
		JobName: job.JobName,
		Targets: myTargets,
	}
}

func (cht *ConsistentHashTargeter) rehashAll() {
	for jobName := range cht.curJobs {
		cht.rehash(jobName)
	}
}

func (cht *ConsistentHashTargeter) hashTargets(jobName JobName, targets map[Instance]Target) map[Instance]Target {
	result := map[Instance]Target{}
	ring := hashring.New(cht.curPool)
	for instance, target := range targets {
		key := fmt.Sprintf("%s%s", jobName, instance)
		id, _ := ring.GetNode(key)
		if id == cht.id {
			result[instance] = target
		}
	}
	return result
}
