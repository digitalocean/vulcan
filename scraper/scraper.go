package scraper

import (
	"sync"

	log "github.com/Sirupsen/logrus"
)

// Scraper answers to a query on which targets are running and groups them
// by job.
type Scraper struct {
	Targeter Targeter
	Writer   Writer
	running  map[JobName]map[Instance]*Worker
	done     chan struct{}
	once     sync.Once
}

// Config represents an instance of a scraper configuration.
// It contains a Targeter interface and a Writer interface.
type Config struct {
	Targeter Targeter
	Writer   Writer
}

// NewScraper returns a new Scraper instance from the provided Config.
func NewScraper(config *Config) *Scraper {
	return &Scraper{
		Targeter: config.Targeter,
		Writer:   config.Writer,
		running:  map[JobName]map[Instance]*Worker{},
		done:     make(chan struct{}),
	}
}

// Run ranges over the Scraper's targets and does work on them.
func (s *Scraper) Run() error {
	for job := range s.Targeter.Targets() {
		s.set(job)
		log.WithField("job_name", job.JobName).WithField("target_count", len(job.Targets)).Info("scraping job")
	}
	return nil
}

func (s *Scraper) set(job Job) {
	workers := s.running[job.JobName]
	next := map[Instance]*Worker{}
	for instance, target := range job.Targets {
		worker, ok := workers[instance]
		if !ok {
			next[instance] = NewWorker(&WorkerConfig{
				JobName:  job.JobName,
				Instance: instance,
				Target:   target,
				Writer:   s.Writer,
			})
			continue
		}
		if !worker.Target.Equals(target) {
			worker.Retarget(target)
		}
		next[instance] = worker
		delete(workers, instance)
	}
	for _, worker := range workers {
		worker.Stop()
	}
	if len(next) == 0 {
		delete(s.running, job.JobName)
		return
	}
	s.running[job.JobName] = next
}

// Stop signals the Scraper instance to stop running.
func (s *Scraper) Stop() {
	s.once.Do(func() {
		close(s.done)
	})
}
