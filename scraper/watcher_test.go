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
	"fmt"
	"time"
)

func NewJobWatcherTester() *JobWatcherTester {
	return &JobWatcherTester{
		out: make(chan []Job),
	}
}

type JobWatcherTester struct {
	out chan []Job
}

func (t *JobWatcherTester) Jobs() <-chan []Job {
	return t.out
}

func (t *JobWatcherTester) SendStaticJobs(n int) {
	for i := 0; i < n; i++ {
		t.out <- getTestScraperStaticJobs(i)
	}
}

func getTestScraperStaticJobs(n int) []Job {
	return []Job{
		&StaticJob{
			jobName: JobName(fmt.Sprintf("job%d-1", n)),
		},
		&StaticJob{
			jobName: JobName(fmt.Sprintf("job%d-2", n)),
		},
		&StaticJob{
			jobName: JobName(fmt.Sprintf("job%d-3", n)),
		},
	}
}

type MockTargetWatcher struct {
	out chan []Targeter
}

func NewMockTargetWatcher() *MockTargetWatcher {
	return &MockTargetWatcher{
		out: make(chan []Targeter),
	}
}

func (t *MockTargetWatcher) Targets() <-chan []Targeter {
	return t.out
}

func (t *MockTargetWatcher) SendHTTPTargeters(count int, delay time.Duration) {
	for i := 0; i < count; i++ {
		t.out <- getTestHTTPTargeters(i)
	}
}

func getTestHTTPTargeters(n int) []Targeter {
	return []Targeter{
		&HTTPTarget{j: JobName(fmt.Sprintf("job-%d-1", n))},
		&HTTPTarget{j: JobName(fmt.Sprintf("job-%d-2", n))},
		&HTTPTarget{j: JobName(fmt.Sprintf("job-%d-3", n))},
	}
}
