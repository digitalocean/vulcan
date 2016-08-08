package scraper

import (
	"math/rand"
	"sync"
	"time"
)

// Worker represents an instance of a scraper worker.
type Worker struct {
	jobName  JobName
	instance Instance
	Target   Target
	last     time.Time
	writer   Writer
	done     chan struct{}
	once     sync.Once
}

// NewWorker creates a new instance of a Worker.
func NewWorker(config *WorkerConfig) *Worker {
	w := &Worker{
		jobName:  config.JobName,
		instance: config.Instance,
		Target:   config.Target,
		writer:   config.Writer,
		done:     make(chan struct{}),
	}
	go w.run()
	return w
}

// WorkerConfig respresents an instance of a Worker's configuration.
type WorkerConfig struct {
	JobName  JobName
	Instance Instance
	Target   Target
	Writer   Writer
}

func (w *Worker) run() {
	splay := time.Duration(rand.Int63n(int64(w.Target.Interval()))) // TODO make this a consistent splay based off of metric name
	ticker := newSplayTicker(splay, w.Target.Interval())
	nowch := ticker.C()
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-nowch:
			fams, err := w.Target.Fetch()
			if err != nil {
				continue // keep trying
			}
			err = w.writer.Write(w.jobName, w.instance, fams)
			if err != nil {

			}

		}
	}
}

// Retarget sets the current Worker's target to the parameter t.
func (w *Worker) Retarget(t Target) {
	w.Target = t
}

// Stop signals the current Worker instance to stop running.
func (w *Worker) Stop() {
	w.once.Do(func() {
		close(w.done)
	})
}
