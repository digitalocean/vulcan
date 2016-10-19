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
	"time"

	"github.com/digitalocean/vulcan/model"

	log "github.com/Sirupsen/logrus"
)

func (d *Downsampler) updateLastWrite(fqmn string, t int64) {
	d.mutex.Lock()
	d.lastWrite[fqmn] = t
	d.stateHashLength.Set(float64(len(d.lastWrite)))
	log.WithFields(log.Fields{
		"last_writes": d.lastWrite,
	}).Debug("updateLastWrite called.")
	d.mutex.Unlock()
}

func (d *Downsampler) updateLastWrites(tsb model.TimeSeriesBatch) {
	d.mutex.Lock()
	for _, ts := range tsb {
		d.lastWrite[ts.ID()] = ts.Samples[0].TimestampMS
	}
	d.stateHashLength.Set(float64(len(d.lastWrite)))
	log.WithFields(log.Fields{
		"last_writes": d.lastWrite,
	}).Debug("updateLastWrites called.")
	d.mutex.Unlock()
}

func (d *Downsampler) getLastWrite(fqmn string) (timestampMS int64, ok bool) {
	d.mutex.Lock()
	t, ok := d.lastWrite[fqmn]
	log.WithFields(log.Fields{
		"last_writes": d.lastWrite,
	}).Debug("getLastWrite called.")
	d.mutex.Unlock()

	return t, ok
}

func (d *Downsampler) cleanLastWrite(now int64, diff int64) {
	d.mutex.Lock()
	for fmqn, ts := range d.lastWrite {
		if now-ts > diff {
			log.WithFields(log.Fields{
				"now":  now,
				"last": ts,
				"diff": diff,
			}).Debug("last write is greater than diff, deleting")
			delete(d.lastWrite, fmqn)
			d.stateHashDeletes.Inc()
		}
	}
	// log.WithFields(log.Fields{
	// 	"last_writes": d.lastWrite,
	// }).Info("cleanLastWrite called.")
	d.mutex.Unlock()
}

func (d *Downsampler) getLastFrDisk(fqmn string) (updatedAtMS int64, err error) {
	d.readCount.WithLabelValues("disk").Inc()
	s, err := d.reader.GetLastSample(fqmn)
	if err != nil {
		return 0, err
	}
	// if record does not exist, we get the a 0 value timestampMS, which should
	// always update.
	return s.TimestampMS, nil
}

// cleanUp sweeps through the downsampler lastWrite state, and removes
// records older than 1.5X the resolution duration.
func (d *Downsampler) cleanUp() {
	var (
		diffd = time.Duration(d.resolution*3/2) * time.Millisecond
		t     = time.NewTicker(diffd)
		diffi = diffd.Nanoseconds() / int64(time.Millisecond)
	)

	for {
		select {
		case <-t.C:
			log.Debug("cleanup interval here.")
			d.cleanLastWrite(timeToMS(time.Now()), diffi)

		case <-d.done:
			t.Stop()
			return
		}
	}
}

func timeToMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
