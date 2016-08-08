package scraper

import (
	"fmt"
	"testing"
)

type mockTargeter struct {
	out chan Job
}

func newMockTargeter() *mockTargeter {
	return &mockTargeter{
		out: make(chan Job),
	}
}

func (mt mockTargeter) Targets() <-chan Job {
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
	mt := newMockTargeter()
	mp := newMockPool()
	cht := NewConsistentHashTargeter(&ConsistentHashTargeterConfig{
		ID:       "abcd",
		Targeter: mt,
		Pool:     mp,
	})
	go func() {
		mp.out <- []string{"1234", "abcd"}
		mt.out <- Job{
			JobName: "test",
			Targets: map[Instance]Target{
				"test-1": &HTTPTarget{},
			},
		}
	}()
	jobs := cht.Targets()

	job := <-jobs
	fmt.Printf("job: %+v\n", job)
}
