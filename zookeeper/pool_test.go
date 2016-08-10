package zookeeper

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func TestPoolRun(t *testing.T) {
	var runValidations = []struct {
		desc       string
		children   []string
		eventDelay int
		closeDelay int
		createErr  error
	}{
		{
			desc:       "event received before close",
			children:   []string{"foo", "bar"},
			eventDelay: 1,
			closeDelay: 5,
		},
		{
			desc:       "close before event recieved",
			children:   []string{"foo", "bar"},
			eventDelay: 5,
			closeDelay: 3,
		},
	}

	for i, test := range runValidations {
		t.Logf("run validation test %d: %q", i, test.desc)

		c := NewZKConn()
		c.EventChannel = make(chan zk.Event)
		c.Children = test.children
		c.Jobs = fmt.Sprintf("somejob%d", i)
		c.CreateErr = test.createErr

		p := &Pool{
			id:   "default-test",
			conn: c.Mock,
			path: "/vulcan/test/scrapers",
			done: make(chan struct{}),
			out:  make(chan []string),
		}

		if test.eventDelay > 0 {
			go c.SendEvent(time.Duration(test.eventDelay) * time.Second)
		}

		if test.closeDelay > 0 {
			go func() {
				time.Sleep(time.Duration(test.closeDelay) * time.Second)
				p.Stop()
			}()
		}

		testCh := make(chan struct{})
		go func() {

			go func() {
				for ch := range p.Scrapers() {
					if !reflect.DeepEqual(ch, test.children) {
						t.Errorf("expected pool targets %v, but got %v", test.children, ch)
					}
				}
			}()

			p.run()
			testCh <- struct{}{}
		}()

		select {
		case <-time.After(time.Duration(test.closeDelay+3) * time.Second):
			t.Errorf(
				"run() => expected but close within %d seconds but time exceeded",
				test.closeDelay+1,
			)
		case <-testCh:
			t.Logf("happy path test %d: passed", i)
		}
	}
}
