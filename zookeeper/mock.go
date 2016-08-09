package zookeeper

import "github.com/samuel/go-zookeeper/zk"

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

func NewMockZK() *MockZK {
	mockStat := &zk.Stat{
		Version: 42,
	}
	return &MockZK{
		Args:       [][]interface{}{},
		ChildrenFn: func(string) ([]string, *zk.Stat, error) { return []string{}, mockStat, nil },
		CreateFn:   func(string, []byte, int32, []zk.ACL) (string, error) { return "", nil },
		DeleteFn:   func(string, int32) error { return nil },
		ExistsFn:   func(string) (bool, *zk.Stat, error) { return false, mockStat, nil },
		GetFn:      func(string) ([]byte, *zk.Stat, error) { return []byte{}, mockStat, nil },
		SetFn:      func(string, []byte, int32) (*zk.Stat, error) { return mockStat, nil },
	}
}

func (mzk *MockZK) Children(path string) ([]string, *zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"children",
		path,
	})
	return mzk.ChildrenFn(path)
}

func (mzk *MockZK) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"childrenw",
		path,
	})
	return mzk.ChildrenwFn(path)
}

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

func (mzk *MockZK) Delete(path string, version int32) error {
	mzk.Args = append(mzk.Args, []interface{}{
		"delete",
		path,
		version,
	})
	return mzk.DeleteFn(path, version)
}

func (mzk *MockZK) Exists(path string) (bool, *zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"exists",
		path,
	})
	return mzk.ExistsFn(path)
}

func (mzk *MockZK) Get(path string) ([]byte, *zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"get",
		path,
	})
	return mzk.GetFn(path)
}

func (mzk *MockZK) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"get",
		path,
	})
	return mzk.GetwFn(path)
}

func (mzk *MockZK) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	mzk.Args = append(mzk.Args, []interface{}{
		"set",
		path,
		data,
		version,
	})
	return mzk.SetFn(path, data, version)
}
