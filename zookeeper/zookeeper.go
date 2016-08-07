package zookeeper

import "github.com/samuel/go-zookeeper/zk"

// Client in a interface that wraps zookeeper client methods.
type Client interface {
	GetW(string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	ChildrenW(string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Create(string, []byte, int32, []zk.ACL) (string, error)
}
