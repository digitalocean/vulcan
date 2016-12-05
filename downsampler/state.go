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
	"sync/atomic"
	"time"

	"github.com/digitalocean/vulcan/model"
)

func (d *Downsampler) appendLastWrite(fqmn string, t int64) {
	d.mutex.Lock()
	d.lastWrite[fqmn] = int64ToPt(t)
	d.mutex.Unlock()
}

func (d *Downsampler) updateLastWrite(fqmn string, t int64) {
	defer func() { d.stateHashLength.Set(float64(d.lenLastWrite())) }()

	d.mutex.Lock()

	a, ok := d.lastWrite[fqmn]
	if !ok {
		// Unlock read mutext before appending to avoid deadlock with appendLastWrite.
		d.mutex.Unlock()
		d.appendLastWrite(fqmn, t)

		return
	}
	atomic.SwapInt64(a, t)

	d.mutex.Unlock()
}

func (d *Downsampler) updateLastWrites(tsb model.TimeSeriesBatch) {
	for _, ts := range tsb {
		d.updateLastWrite(ts.ID(), ts.Samples[0].TimestampMS)
	}
}

func (d *Downsampler) getLastWriteValue(fqmn string) (timestampMS int64, ok bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	a, ok := d.lastWrite[fqmn]
	if !ok {
		return 0, ok
	}

	return atomic.LoadInt64(a), ok
}

func (d *Downsampler) cleanLastWrite(now int64, diff int64) {
	var toDelete = map[string]int64{}

	d.mutex.RLock()
	for fqmn, ts := range d.lastWrite {
		if now-*ts > diff {
			toDelete[fqmn] = *ts
		}
	}
	d.mutex.RUnlock()

	if len(toDelete) > 0 {
		d.mutex.Lock()
		for fqmn, ts := range toDelete {
			// Only delete if the timestamp of the fqmn is still the same one that
			// we measured against when we marked the item for deletion.
			if ts == *d.lastWrite[fqmn] {
				delete(d.lastWrite, fqmn)
			}
		}
		d.mutex.Unlock()
	}

	d.stateHashDeletes.Add(float64(len(toDelete)))
}

func (d *Downsampler) getLastFrDisk(fqmn string) (updatedAtMS int64, err error) {
	t0 := time.Now()
	defer func() {
		d.batchProcessDistribution.WithLabelValues("cassandra_read").Add(time.Since(t0).Seconds())
	}()

	d.readCount.WithLabelValues("disk").Inc()

	s, err := d.reader.GetLastSample(fqmn)
	if err != nil {
		return 0, err
	}

	return s.TimestampMS, nil
}

// cleanUp sweeps through the downsampler lastWrite state, and removes
// records older than the configured resolution duration.
func (d *Downsampler) cleanUp() {
	var (
		diffd = time.Duration(float64(d.resolution)*d.cleanupRate) * time.Millisecond
		t     = time.NewTicker(2 * diffd)
		diffi = diffd.Nanoseconds() / int64(time.Millisecond)
		first = true
	)

	for {
		select {
		case <-t.C:
			if first {
				// after first try reset cleanup duration to original configuration.
				t.Stop()
				t = time.NewTicker(diffd)
				first = false
			}

			d.cleanLastWrite(timeToMS(time.Now()), diffi)

		case <-d.done:
			t.Stop()
			return
		}
	}
}

func (d *Downsampler) lenLastWrite() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return len(d.lastWrite)
}

func timeToMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func int64ToPt(i int64) *int64 {
	return &i
}
