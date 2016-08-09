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
	MockChildren []string
	Jobs         string
}

// GetW is a stubbed version of zk.GetW.
func (c *ZKConn) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if c.GetErr != nil {
		return nil, nil, nil, c.GetErr
	}
	return []byte(c.Jobs), c.Stat, c.EventChannel, nil
}

// ChildrenW is a stubbed versionof zk.ChildrenW.
func (c *ZKConn) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if c.ChildrenWErr != nil {
		return nil, nil, nil, c.ChildrenWErr
	}
	return c.MockChildren, c.Stat, c.EventChannel, c.ChildrenWErr
}

// Create is a stubbed version of zk.Create.
func (c *ZKConn) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if c.CreateErr != nil {
		return "", c.CreateErr
	}
	return path, nil
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
