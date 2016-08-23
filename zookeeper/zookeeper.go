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

import "github.com/samuel/go-zookeeper/zk"

// Client in a interface that wraps zookeeper client methods.
type Client interface {
	Children(path string) ([]string, *zk.Stat, error)
	ChildrenW(string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Delete(path string, version int32) error
	Exists(path string) (bool, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
	GetW(string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
}
