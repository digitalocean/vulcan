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
