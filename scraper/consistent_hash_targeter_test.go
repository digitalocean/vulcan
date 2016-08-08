package scraper

import (
	"fmt"
	"testing"
)

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
	fmt.Printf("job: %+v\n", job)
}
