package scraper

import (
	"errors"
	"testing"
	"time"
)

func TestWorkerRun(t *testing.T) {
	var runValidations = []struct {
		desc       string
		closeDelay time.Duration
		writerErr  error
		targetErr  error
	}{
		{
			desc:       "no errors",
			closeDelay: time.Duration(2) * time.Second,
		},
		{
			desc:       "fetch error",
			closeDelay: time.Duration(2) * time.Second,
			targetErr:  errors.New(TESTFETCHERR),
		},
		{
			desc:       "write errors",
			closeDelay: time.Duration(2) * time.Second,
			writerErr:  errors.New(TESTWRITEERR),
		},
	}

	for i, test := range runValidations {
		t.Logf("run validations %d: %q", i, test.desc)

		w := &Worker{
			key: "test.example.com",
			Target: &MockTargeter{
				i:                 time.Duration(30) * time.Second,
				fetchErr:          test.targetErr,
				numOfFetchMetrics: 1,
			},
			writer: &MockWriter{WErr: test.writerErr},
			done:   make(chan struct{}),
		}

		runCh := make(chan struct{})
		go func() {
			w.run()
			close(runCh)
		}()

		go func() {
			time.Sleep(test.closeDelay)
			w.Stop()
		}()

		select {
		case <-runCh:
			t.Logf("run validation %d: passed", i)
		case <-time.After(test.closeDelay + 300*time.Millisecond):
			t.Errorf(
				"run() => did not exit by expected time of %v",
				test.closeDelay,
			)
		}
	}
}
