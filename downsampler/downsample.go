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

package downsampler

import (
	"sync"
	"time"

	"github.com/digitalocean/vulcan/bus"
	"github.com/digitalocean/vulcan/cassandra"
	"github.com/digitalocean/vulcan/ingester"
	"github.com/digitalocean/vulcan/model"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "vulcan"
	subsystem = "downsampler"
)

// Downsampler reads from a kafka topics and records each consumed timeseries
// at the configured resolution.  First check is applied on an in memory cache;
// if the time series does nto exist, the next checked against a disk storage
// using the passed reader inteface.
type Downsampler struct {
	prometheus.Collector

	consumer bus.Source
	writer   ingester.Writer
	reader   cassandra.Reader

	// the resolution type int64 matches the type for the timestamp we get
	// off of the bus, and write to cassandra with to avoid unnecessary type
	// conversions.
	resolution int64

	cleanupF func()

	lastWrite map[string]int64

	done  chan struct{}
	mutex *sync.Mutex
	once  *sync.Once

	stateHashLength  prometheus.Gauge
	stateHashDeletes prometheus.Counter
	writeCount       prometheus.Counter
	readCount        *prometheus.CounterVec
}

// Config represents the configurable attributes of a Downsampler instance.
type Config struct {
	Consumer    bus.Source
	Writer      ingester.Writer
	Reader      cassandra.Reader
	Resolution  time.Duration
	CleanupFunc func()
}

// NewDownsampler returns a new instance of a Downsampler.
func NewDownsampler(config *Config) *Downsampler {
	d := &Downsampler{
		consumer:   config.Consumer,
		writer:     config.Writer,
		reader:     config.Reader,
		resolution: config.Resolution.Nanoseconds() / int64(time.Millisecond),
		cleanupF:   config.CleanupFunc,

		lastWrite: map[string]int64{},

		done:  make(chan struct{}),
		mutex: new(sync.Mutex),
		once:  new(sync.Once),

		stateHashLength: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "state_hash_length",
				Help:      "Length of state hash map",
			},
		),
		writeCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "write_count_total",
				Help:      "Count of writes made to sample storage",
			},
		),
		stateHashDeletes: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "state_hash_deletes_total",
				Help:      "Count of number of hash member deletes during cleanup cycle",
			},
		),
		readCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "read_count_total",
				Help:      "Reads made for sample state in memory or from disk",
			},
			[]string{"type"},
		),
	}

	return d
}

// Describe implements prometheus.Collector which makes the downsampler
// registrable to prometheus instrumentation.s
func (d *Downsampler) Describe(ch chan<- *prometheus.Desc) {
	d.stateHashLength.Describe(ch)
	d.writeCount.Describe(ch)
	d.readCount.Describe(ch)
	d.stateHashDeletes.Describe(ch)
}

// Collect implements prometheus.Collector which makes the downsampler
// registrable to prometheus instrumentation.
func (d *Downsampler) Collect(ch chan<- prometheus.Metric) {
	d.stateHashLength.Collect(ch)
	d.writeCount.Collect(ch)
	d.readCount.Collect(ch)
	d.stateHashDeletes.Collect(ch)
}

// Run starts the downsampling process.  Exits with error on first encountered
// error.
func (d *Downsampler) Run(numWorkers int) error {
	var (
		wg     sync.WaitGroup
		runErr error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		d.cleanUp()
	}()

	wg.Add(numWorkers)
	for n := 0; n < numWorkers; n++ {
		go func() {
			defer wg.Done()

			if err := d.work(); err != nil {
				runErr = err
				d.Stop()
			}
		}()
	}
	wg.Wait()

	return runErr
}

func (d *Downsampler) work() error {
	for {
		select {
		case <-d.done:
			return nil

		case m := <-d.consumer.Messages():
			log.WithFields(log.Fields{
				"tsb": m.TimeSeriesBatch,
			}).Debug("consumer message received")
			if err := d.processTSBatch(m.TimeSeriesBatch); err != nil {
				log.WithFields(log.Fields{
					"timeseries_batch": m.TimeSeriesBatch,
				}).WithError(err).Error("error occurred while processing batch.")

				return err
			}

			m.Ack()
		}
	}
}

func (d *Downsampler) processTSBatch(tsb model.TimeSeriesBatch) error {
	toWrite := make(model.TimeSeriesBatch, 0)

	for _, ts := range tsb {
		log.WithFields(log.Fields{
			"timeseries_samples": ts.Samples,
			"timeseries_fqmn":    ts.ID(),
		}).Debug("checking if timeseries should be written")
		should, s, err := d.shouldWrite(ts)
		if err != nil {
			return err
		}
		if should {
			log.WithFields(log.Fields{
				"timeseries_samples": ts.Samples,
				"timeseries_fqmn":    ts.ID(),
			}).Debug("timeseries should be written.  Appending to batch write")
			toWrite = append(toWrite, &model.TimeSeries{
				Labels:  ts.Labels,
				Samples: []*model.Sample{s},
			})
		}
	}

	if len(toWrite) < 1 {
		log.WithFields(log.Fields{
			"to_write": toWrite,
		}).Debug("nothing to write")
		return nil
	}

	log.WithFields(log.Fields{
		"to_write": toWrite,
	}).Debug("writing batch to storage")
	return d.write(toWrite)
}

// shouldWrite checks if a TimeSeries needs to be written
func (d *Downsampler) shouldWrite(ts *model.TimeSeries) (bool, *model.Sample, error) {
	var (
		t    int64
		err  error
		fqmn = ts.ID()
	)

	ll := log.WithFields(log.Fields{
		"timeseries_samples": ts.Samples,
		"timeseries_fqmn":    ts.ID(),
	})
	t, ok := d.getLastWrite(fqmn)
	if !ok {
		ll.Debug("timeseries not in memory cache, checking from disk")
		t, err = d.getLastFrDisk(fqmn)
		if err != nil {
			return false, nil, err
		}
		ll.WithFields(log.Fields{
			"sample_ts_ms": t,
		}).Debug("got sample from disk.  Updating memory cache")

		// update state
		d.updateLastWrite(fqmn, t)
	} else {
		d.readCount.WithLabelValues("memory").Inc()
	}
	// Sort samples by timestamp and check state against earliest collected
	// sample and write latest collected sample.
	model.SampleSorter(model.SortSampleByTS).Sort(ts.Samples)

	ll.WithFields(log.Fields{
		"current_time": ts.Samples[0].TimestampMS,
		"last_time":    t,
		"resolution":   d.resolution,
	}).Debug("comparing time")
	if ts.Samples[0].TimestampMS-t > d.resolution {
		ll.Debug("time diff is greater than resoution, should write")
		// Return the latest collected sample.
		// Do not update until we know there is a successful write.
		return true, ts.Samples[len(ts.Samples)-1], nil
	}
	ll.Debug("time diff is not greater than resoution, should NOT write")
	return false, nil, nil
}

func (d *Downsampler) write(tsb model.TimeSeriesBatch) error {
	d.writeCount.Add(float64(len(tsb)))
	if err := d.writer.Write(tsb); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"timeseries_batch": tsb,
	}).Debug("write successful.  updating memory cache")
	// Update state now that we know writes are successful.
	d.updateLastWrites(tsb)

	return nil
}

// Stop gracesfully stops the Downsampler and relieves all of its resources.
func (d *Downsampler) Stop() {
	d.once.Do(func() {
		if d.cleanupF != nil {
			d.cleanupF()
		}
		close(d.done)
	})
}
