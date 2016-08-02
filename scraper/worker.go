package scraper

import (
	"math/rand"
	"sync"
	"time"
)

type Worker struct {
	jobName  JobName
	instance Instance
	Target   Target
	last     time.Time
	writer   Writer
	done     chan struct{}
	once     sync.Once
}

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

type WorkerConfig struct {
	JobName  JobName
	Instance Instance
	Target   Target
	Writer   Writer
}

func (w Worker) run() {
	splay := time.Duration(rand.Int63n(int64(w.Target.Interval()))) // TODO make this a consistent splay based off of metric name
	ticker := NewSplayTicker(splay, w.Target.Interval())
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

func (w Worker) Retarget(t Target) {
	w.Target = t
}

func (w Worker) Stop() {
	w.once.Do(func() {
		close(w.done)
	})
}
