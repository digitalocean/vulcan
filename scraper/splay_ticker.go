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
