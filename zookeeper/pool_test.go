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
		c.ChildrenEventChannel = make(chan zk.Event)
		c.Children = test.children
		c.Jobs = map[string]string{fmt.Sprintf("somejob%d", i): ""}
		c.CreateErr = test.createErr

		testPath := "/vulcan/test/scrapers"

		p := &Pool{
			id:   "default-test",
			conn: c.Mock,
			path: testPath,
			done: make(chan struct{}),
			out:  make(chan []string),
		}

		if test.eventDelay > 0 {
			go c.SendChildrenEvent(time.Duration(test.eventDelay)*time.Second, testPath)
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
