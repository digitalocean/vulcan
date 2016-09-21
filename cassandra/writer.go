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

package cassandra

import (
	"sync"

	"github.com/digitalocean/vulcan/model"
	"github.com/gocql/gocql"
)

// Writer implements ingester.Writer to persist TimeSeriesBatch samples
// to Cassandra.
type Writer struct {
	s  *gocql.Session
	ch chan *writerPayload
}

type writerPayload struct {
	wg    *sync.WaitGroup
	ts    *model.TimeSeries
	errch chan error
}

// WriterConfig specifies how many goroutines should be used in writing
// TimeSeries to Cassandra. The Session is expected to be already created
// and ready to use.
type WriterConfig struct {
	NumWorkers int
	Session    *gocql.Session
}

// NewWriter creates a Writer and starts the configured number of
// goroutines to write to Cassandra concurrently.
func NewWriter(config *WriterConfig) *Writer {
	w := &Writer{
		s:  config.Session,
		ch: make(chan *writerPayload),
	}
	for n := 0; n < config.NumWorkers; n++ {
		go w.worker()
	}
	return w
}

// Write implements the ingester.Write interface and allows the
// ingester to write TimeSeriesBatch to Cassandra.
func (w *Writer) Write(tsb model.TimeSeriesBatch) error {
	wg := &sync.WaitGroup{}
	errch := make(chan error, 1) // room for just the first error a worker encounters
	wg.Add(len(tsb))
	for _, ts := range tsb {
		wp := &writerPayload{
			wg:    wg,
			ts:    ts,
			errch: errch,
		}
		w.ch <- wp
	}
	wg.Wait()
	select {
	case err := <-errch:
		return err
	default:
		return nil
	}
}

func (w *Writer) worker() {
	for m := range w.ch {
		id := m.ts.ID()
		for _, s := range m.ts.Samples {
			err := w.write(id, s.TimestampMS, s.Value)
			if err != nil {
				// send error back on payload's errch; don't block the worker
				select {
				case m.errch <- err:
				default:
				}
			}
		}
		m.wg.Done()
	}
}

func (w *Writer) write(id string, at int64, value float64) error {
	return w.s.Query(writeSampleCQL, value, id, at).Exec()
}
