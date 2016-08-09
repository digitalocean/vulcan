package zookeeper

import (
	"reflect"
	"strings"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

type mockZK struct {
	args     [][]interface{}
	children func(path string) ([]string, *zk.Stat, error)
	create   func(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	delete   func(path string, version int32) error
	exists   func(path string) (bool, *zk.Stat, error)
	get      func(path string) ([]byte, *zk.Stat, error)
	set      func(path string, data []byte, version int32) (*zk.Stat, error)
}

func newMockZK() *mockZK {
	mockStat := &zk.Stat{
		Version: 42,
	}
	return &mockZK{
		args:     [][]interface{}{},
		children: func(string) ([]string, *zk.Stat, error) { return []string{}, mockStat, nil },
		create:   func(string, []byte, int32, []zk.ACL) (string, error) { return "", nil },
		delete:   func(string, int32) error { return nil },
		exists:   func(string) (bool, *zk.Stat, error) { return false, mockStat, nil },
		get:      func(string) ([]byte, *zk.Stat, error) { return []byte{}, mockStat, nil },
		set:      func(string, []byte, int32) (*zk.Stat, error) { return mockStat, nil },
	}
}

func (mzk *mockZK) Children(path string) ([]string, *zk.Stat, error) {
	mzk.args = append(mzk.args, []interface{}{
		"children",
		path,
	})
	return mzk.children(path)
}

func (mzk *mockZK) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	mzk.args = append(mzk.args, []interface{}{
		"create",
		path,
		data,
		flags,
		acl,
	})
	return mzk.create(path, data, flags, acl)
}

func (mzk *mockZK) Delete(path string, version int32) error {
	mzk.args = append(mzk.args, []interface{}{
		"delete",
		path,
		version,
	})
	return mzk.delete(path, version)
}

func (mzk *mockZK) Exists(path string) (bool, *zk.Stat, error) {
	mzk.args = append(mzk.args, []interface{}{
		"exists",
		path,
	})
	return mzk.exists(path)
}

func (mzk *mockZK) Get(path string) ([]byte, *zk.Stat, error) {
	mzk.args = append(mzk.args, []interface{}{
		"get",
		path,
	})
	return mzk.get(path)
}

func (mzk *mockZK) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	mzk.args = append(mzk.args, []interface{}{
		"set",
		path,
		data,
		version,
	})
	return mzk.set(path, data, version)
}

func TestStore(t *testing.T) {
	tests := []struct {
		cluster        string
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
			cluster: "test-cluster",
			root:    "/my/root",
			name:    "test-job",
			value:   []byte("hi"),
			get: [][]interface{}{[]interface{}{
				"get",
				"/my/root/scraper/test-cluster/jobs/test-job",
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
					"/my/root/scraper/test-cluster",
				},
				[]interface{}{
					"exists",
					"/my/root/scraper/test-cluster/jobs",
				},
				[]interface{}{
					"exists",
					"/my/root/scraper/test-cluster/jobs/test-job",
				},
				[]interface{}{
					"set",
					"/my/root/scraper/test-cluster/jobs/test-job",
					[]byte("hi"),
					int32(42),
				},
			},
			delete: [][]interface{}{
				[]interface{}{
					"exists",
					"/my/root/scraper/test-cluster/jobs/test-job",
				},
				[]interface{}{
					"delete",
					"/my/root/scraper/test-cluster/jobs/test-job",
					int32(42),
				},
			},
		},
	}
	for _, test := range tests {
		mzk := newMockZK()
		mzk.exists = func(path string) (bool, *zk.Stat, error) {
			if strings.Contains(path, test.root) {
				return true, &zk.Stat{Version: 42}, nil
			}
			return false, nil, nil
		}
		s, err := NewStore(&Config{
			Cluster: test.cluster,
			Client:  mzk,
			Root:    test.root,
		})
		if err != nil {
			t.Fatal(err)
		}
		s.Get(test.name)
		if !reflect.DeepEqual(mzk.args, test.get) {
			t.Errorf("wanted %+v but got %+v", test.get, mzk.args)
		}
		mzk.args = [][]interface{}{}
		s.Set(test.name, test.value)
		if !reflect.DeepEqual(mzk.args, test.set) {
			t.Errorf("wanted \n%+v\n but got \n%+v\n", test.set, mzk.args)
		}
		mzk.args = [][]interface{}{}
		s.Delete(test.name)
		if !reflect.DeepEqual(mzk.args, test.delete) {
			t.Errorf("wanted \n%+v\n but got \n%+v\n", test.delete, mzk.args)
		}
	}
}
