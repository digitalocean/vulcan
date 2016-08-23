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
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// Below are the stubbed error messages for their corresponding errors
const (
	GetErrMsg       = `ZKCONN GET ERROR`
	CreateErrMsg    = `ZKCONN CREATE ERROR`
	ChildrenWErrMsg = `ZKCONN CHILDRENW ERROR`
)

// ZKConn represents a test version of a Zookeeper client connections.
type ZKConn struct {
	ChildrenEventChannel chan zk.Event
	GetEventChannel      chan zk.Event
	Stat                 *zk.Stat
	GetErr               error
	ChildrenWErr         error
	CreateErr            error
	Children             []string
	Jobs                 map[string]string
	Mock                 Client
}

func NewZKConn() *ZKConn {
	zkc := &ZKConn{}
	mzk := NewMockZK()
	mzk.GetwFn = func(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
		if zkc.GetErr != nil {
			return nil, nil, nil, zkc.GetErr
		}
		return []byte(zkc.Jobs[path]), zkc.Stat, zkc.GetEventChannel, nil
	}

	mzk.ChildrenwFn = func(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
		if zkc.ChildrenWErr != nil {
			return nil, nil, nil, zkc.ChildrenWErr
		}
		return zkc.Children, zkc.Stat, zkc.ChildrenEventChannel, zkc.ChildrenWErr
	}

	mzk.CreateFn = func(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
		if zkc.CreateErr != nil {
			return "", zkc.CreateErr
		}
		return path, nil
	}

	zkc.Mock = mzk

	return zkc
}

// SendChildrenEvent sends an event to the event channel for ChildrenW after
// the provided delay.
func (c *ZKConn) SendChildrenEvent(delay time.Duration, path string) {
	time.Sleep(delay)
	c.ChildrenEventChannel <- zk.Event{
		Type:   1,
		State:  1,
		Path:   path,
		Err:    nil,
		Server: "foobar.com:2181",
	}
}

// SendGetEvent sends an event to the event channel for ChildrenW after
// the provided delay.
func (c *ZKConn) SendGetEvent(delay time.Duration, path string) {
	time.Sleep(delay)
	c.GetEventChannel <- zk.Event{
		Type:   1,
		State:  1,
		Path:   path,
		Err:    nil,
		Server: "foobar.com:2181",
	}
}
