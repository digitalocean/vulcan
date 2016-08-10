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
	EventChannel chan zk.Event
	Stat         *zk.Stat
	GetErr       error
	ChildrenWErr error
	CreateErr    error
	Children     []string
	Jobs         string
	Mock         Client
}

func NewZKConn() *ZKConn {
	zkc := &ZKConn{}
	mzk := NewMockZK()
	mzk.GetwFn = func(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
		if zkc.GetErr != nil {
			return nil, nil, nil, zkc.GetErr
		}
		return []byte(zkc.Jobs), zkc.Stat, zkc.EventChannel, nil
	}
	mzk.ChildrenwFn = func(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
		if zkc.ChildrenWErr != nil {
			return nil, nil, nil, zkc.ChildrenWErr
		}
		return zkc.Children, zkc.Stat, zkc.EventChannel, zkc.ChildrenWErr
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

// SendEvent sends an event to the event channel after the provided delay.
func (c *ZKConn) SendEvent(delay time.Duration) {
	time.Sleep(delay)
	c.EventChannel <- zk.Event{
		Type:   1,
		State:  1,
		Path:   "/zkroot/some/thing",
		Err:    nil,
		Server: "foobar.com:2181",
	}
}
