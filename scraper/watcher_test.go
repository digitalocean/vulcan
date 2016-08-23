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
