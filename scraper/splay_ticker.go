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
	"time"
)

type splayTicker struct {
	ch     chan time.Time
	done   chan struct{}
	ticker *time.Ticker
	once   sync.Once
}

func newSplayTicker(splay, interval time.Duration) *splayTicker {
	st := &splayTicker{
		ch:   make(chan time.Time),
		done: make(chan struct{}),
	}
	time.AfterFunc(splay, func() {
		// send tick at start of splay
		select {
		case st.ch <- time.Now():
		default:
		}
		ticker := time.NewTicker(interval)
		defer func() {
			ticker.Stop()
		}()
		for {
			select {
			case <-st.done:
				return
			case t := <-ticker.C:
				select {
				case st.ch <- t:
					continue
				default:
					// similar to ticker functionality, if the receiver is not ready to
					// consume the tick, then skip this tick
					continue
				}
			}
		}
	})
	return st
}

func (st *splayTicker) C() <-chan time.Time {
	return st.ch
}

func (st *splayTicker) Stop() {
	st.once.Do(func() {
		close(st.done)
	})
}
