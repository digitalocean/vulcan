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

import "testing"

type mockTargeter struct {
	out chan []Targeter
}

func newMockTargeter() *mockTargeter {
	return &mockTargeter{
		out: make(chan []Targeter),
	}
}

func (mt mockTargeter) Targets() <-chan []Targeter {
	return mt.out
}

type mockPool struct {
	out chan []string
}

func newMockPool() *mockPool {
	return &mockPool{
		out: make(chan []string),
	}
}

func (mp mockPool) Scrapers() <-chan []string {
	return mp.out
}

func TestConsistentHashTargeter(t *testing.T) {
	mt := NewMockTargetWatcher()
	mp := newMockPool()
	cht := NewConsistentHashTargeter(&ConsistentHashTargeterConfig{
		ID:       "abcd",
		Targeter: mt,
		Pool:     mp,
	})
	go func() {
		mp.out <- []string{"1234", "abcd"}
		mt.out <- []Targeter{
			&HTTPTarget{
				j: JobName("test"),
			},
		}
	}()
	jobs := cht.Targets()

	job := <-jobs
	t.Logf("job: %+v\n", job)
}
