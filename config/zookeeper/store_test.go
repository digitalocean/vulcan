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
	"reflect"
	"strings"
	"testing"

	"github.com/digitalocean/vulcan/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
)

func TestStore(t *testing.T) {
	tests := []struct {
		root           string
		name           string
		value          []byte
		expectBasePath string
		expectNamePath string
		get            [][]interface{}
		set            [][]interface{}
		delete         [][]interface{}
	}{
		{
			root:  "/my/root",
			name:  "test-job",
			value: []byte("hi"),
			get: [][]interface{}{[]interface{}{
				"get",
				"/my/root/scraper/jobs/test-job",
			}},
			set: [][]interface{}{
				[]interface{}{
					"exists",
					"/my",
				},
				[]interface{}{
					"create",
					"/my",
					[]byte{},
					int32(0),
					zk.WorldACL(zk.PermAll),
				},
				[]interface{}{
					"exists",
					"/my/root",
				},
				[]interface{}{
					"exists",
					"/my/root/scraper",
				},
				[]interface{}{
					"exists",
					"/my/root/scraper/jobs",
				},
				[]interface{}{
					"exists",
					"/my/root/scraper/jobs/test-job",
				},
				[]interface{}{
					"set",
					"/my/root/scraper/jobs/test-job",
					[]byte("hi"),
					int32(42),
				},
			},
			delete: [][]interface{}{
				[]interface{}{
					"exists",
					"/my/root/scraper/jobs/test-job",
				},
				[]interface{}{
					"delete",
					"/my/root/scraper/jobs/test-job",
					int32(42),
				},
			},
		},
	}
	for _, test := range tests {
		mzk := zookeeper.NewMockZK()
		mzk.ExistsFn = func(path string) (bool, *zk.Stat, error) {
			if strings.Contains(path, test.root) {
				return true, &zk.Stat{Version: 42}, nil
			}
			return false, nil, nil
		}
		s, err := NewStore(&Config{
			Client: mzk,
			Root:   test.root,
		})
		if err != nil {
			t.Fatal(err)
		}
		s.Get(test.name)
		if !reflect.DeepEqual(mzk.Args, test.get) {
			t.Errorf("wanted %+v but got %+v", test.get, mzk.Args)
		}
		mzk.Args = [][]interface{}{}
		s.Set(test.name, test.value)
		if !reflect.DeepEqual(mzk.Args, test.set) {
			t.Errorf("wanted \n%+v\n but got \n%+v\n", test.set, mzk.Args)
		}
		mzk.Args = [][]interface{}{}
		s.Delete(test.name)
		if !reflect.DeepEqual(mzk.Args, test.delete) {
			t.Errorf("wanted \n%+v\n but got \n%+v\n", test.delete, mzk.Args)
		}
	}
}
