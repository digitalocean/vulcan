package zookeeper

import (
	"path"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

type zookeeper interface {
	Children(path string) ([]string, *zk.Stat, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Delete(path string, version int32) error
	Exists(path string) (bool, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
}

// Store is implemented on top of Zookeeper
type Store struct {
	cluster string
	client  zookeeper
	root    string
}

// Config defines the scraper cluster and the zookeeper root so that we look
// at the right data in zookeeper.
type Config struct {
	Cluster string
	Client  zookeeper
	Root    string
}

// NewStore creates a store from the config. It provides a default root value if
// the zero value was set in the Config.
func NewStore(c *Config) (*Store, error) {
	root := "/"
	if c.Root != "" {
		root = c.Root
	}
	return &Store{
		cluster: c.Cluster,
		client:  c.Client,
		root:    root,
	}, nil
}

// Delete removes a key from zookeeper that defines a job
func (s *Store) Delete(name string) error {
	p := s.path(name)
	exists, stat, err := s.client.Exists(p)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return s.client.Delete(p, stat.Version)
}

// List shows all job names in zookeeper for the configured scraper cluster
func (s *Store) List() ([]string, error) {
	c, _, err := s.client.Children(s.basePath())
	return c, err
}

// Get returns the raw bytes in zookeeper for the job
func (s *Store) Get(name string) ([]byte, error) {
	b, _, err := s.client.Get(s.path(name))
	return b, err
}

// Set creates or overwrites the key in zookeeper with the provided name
func (s *Store) Set(name string, b []byte) error {
	err := s.ensurePath(s.basePath())
	if err != nil {
		return err
	}
	p := s.path(name)
	exists, stat, err := s.client.Exists(p)
	if err != nil {
		return err
	}
	if exists {
		_, err = s.client.Set(p, b, stat.Version)
		return err
	}
	_, err = s.client.Create(p, b, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (s *Store) basePath() string {
	return path.Join(s.root, "scraper", s.cluster, "jobs")
}

func (s *Store) ensurePath(p string) error {
	// p must start with "/" so first element of split is always "" and skippable
	parts := strings.Split(p, "/")[1:]
	acc := ""
	for _, part := range parts {
		acc = acc + "/" + part
		log.WithFields(log.Fields{
			"path": acc,
		}).Debug("ensuring path exists")
		exists, _, err := s.client.Exists(acc)
		if err != nil {
			return err
		}
		if exists {
			log.WithFields(log.Fields{
				"path": acc,
			}).Debug("path already exists")
			continue
		}
		_, err = s.client.Create(acc, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) path(name string) string {
	return path.Join(s.basePath(), name)
}
