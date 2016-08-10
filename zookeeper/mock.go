package zookeeper

import "github.com/samuel/go-zookeeper/zk"

// MockZK implements the zookeeper interface with overwritable implementation
// functions so that tests can supply their own behaviour. It is public so that
// it can be reused among other packages.
type MockZK struct {
	Args        [][]interface{}
	ChildrenFn  func(path string) ([]string, *zk.Stat, error)
	ChildrenwFn func(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	CreateFn    func(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	DeleteFn    func(path string, version int32) error
	ExistsFn    func(path string) (bool, *zk.Stat, error)
	GetFn       func(path string) ([]byte, *zk.Stat, error)
	GetwFn      func(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	SetFn       func(path string, data []byte, version int32) (*zk.Stat, error)
}

// NewMockZK returns a MockZK that has default implementations for all overridable
// functions.
func NewMockZK() *MockZK {
	mockStat := &zk.Stat{
		Version: 42,
	}
	return &MockZK{
		Args:       [][]interface{}{},
		ChildrenFn: func(string) ([]string, *zk.Stat, error) { return []string{}, mockStat, nil },
		ChildrenwFn: func(string) ([]string, *zk.Stat, <-chan zk.Event, error) {
			return []string{}, mockStat, make(chan zk.Event), nil
		},
		CreateFn: func(string, []byte, int32, []zk.ACL) (string, error) { return "", nil },
		DeleteFn: func(string, int32) error { return nil },
		ExistsFn: func(string) (bool, *zk.Stat, error) { return false, mockStat, nil },
		GetFn:    func(string) ([]byte, *zk.Stat, error) { return []byte{}, mockStat, nil },
		GetwFn: func(string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
			return []byte{}, mockStat, make(chan zk.Event), nil
		},
		SetFn: func(string, []byte, int32) (*zk.Stat, error) { return mockStat, nil },
	}
}

// Children returns the files contained at a given path
func (mzk *MockZK) Children(path string) ([]string, *zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"children",
		path,
	})
	return mzk.ChildrenFn(path)
}

// ChildrenW returns the files contained at a given path plus a channel that
// will contain a zk.Event when a change occurs at that path.
func (mzk *MockZK) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"childrenw",
		path,
	})
	return mzk.ChildrenwFn(path)
}

// Create puts data into zookeeper into a path when a key did not previously exists
// at that path.
func (mzk *MockZK) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"create",
		path,
		data,
		flags,
		acl,
	})
	return mzk.CreateFn(path, data, flags, acl)
}

// Delete removes a key from zookeeper that already exists.
func (mzk *MockZK) Delete(path string, version int32) error {
	mzk.Args = append(mzk.Args, []interface{}{
		"delete",
		path,
		version,
	})
	return mzk.DeleteFn(path, version)
}

// Exists returns true when a path exists; it also returns a zk.Stats which
// may be needed for future calls to operate on that same path.
func (mzk *MockZK) Exists(path string) (bool, *zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"exists",
		path,
	})
	return mzk.ExistsFn(path)
}

// Get returns the data and zk.Stat for a given path.
func (mzk *MockZK) Get(path string) ([]byte, *zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"get",
		path,
	})
	return mzk.GetFn(path)
}

// GetW returns the data and zk.Stat for a given path plus a channel that will
// populate with a zk.Event when the data at that path is altered.
func (mzk *MockZK) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"getw",
		path,
	})
	return mzk.GetwFn(path)
}

// Set writes data into zookeeper at a given path where the path already contained
// data.
func (mzk *MockZK) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"set",
		path,
		data,
		version,
	})
	return mzk.SetFn(path, data, version)
}
