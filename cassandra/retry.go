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
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

var _ gocql.RetryPolicy = &SimpleTimedRetry{}

// SimpleTimedRetry retries for X number of times blocking for Y duration.
// If retried times exceeds X, returns false.  If stop is called, Attempt calls
// always returns false and retry resets no longer occur.
type SimpleTimedRetry struct {
	retries int32
	retried *int32

	blockDuration time.Duration
	resetDuration time.Duration

	done chan struct{}
	once *sync.Once
}

// SimpleRetryConfig represents the configuration of a SimpleTimedRetry.
type SimpleRetryConfig struct {
	Retries       int
	BlockDuration time.Duration
	ResetDuration time.Duration
}

// NewSimpleTimedRetry returns a new instance of SimpleTimedRetry.
func NewSimpleTimedRetry(config *SimpleRetryConfig) *SimpleTimedRetry {
	s := &SimpleTimedRetry{
		retries:       int32(config.Retries),
		retried:       func(i int32) *int32 { return &i }(0),
		blockDuration: config.BlockDuration,
		resetDuration: config.ResetDuration,

		once: new(sync.Once),
	}

	go s.resetLoop()

	return s
}

func (s *SimpleTimedRetry) resetLoop() {
	t := time.NewTicker(s.resetDuration)

	for {
		select {
		case <-t.C:
			// only cleanup if retry limit is not exceeded.
			if atomic.LoadInt32(s.retried) <= s.retries {
				atomic.StoreInt32(s.retried, 0)
			}

		case <-s.done:
			t.Stop()
			return
		}
	}
}

// Attempt blocks for the configured duration if retry limit is not exceeded and
// returns false immediately if limit is exceeded
func (s *SimpleTimedRetry) Attempt(q gocql.RetryableQuery) bool {
	if atomic.LoadInt32(s.retried) < s.retries {
		// Mark retry attempt right now so subsequent calls during block period
		// know if retry attempts have been exceeded
		atomic.AddInt32(s.retried, 1)

		t := time.NewTicker(s.blockDuration)
		select {
		case <-t.C:
			return true

		case <-s.done:
			t.Stop()
			return false
		}
	}

	return false
}

// Stop gracefully stops SimpleTimedRetry.
func (s *SimpleTimedRetry) Stop() {
	s.once.Do(func() {
		close(s.done)
	})
}
