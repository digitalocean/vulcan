package zookeeper

import (
	"path"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

// Targeter uses zookeeper as a backend for configuring jobs that vulcan should scrape.
// The targeter watches the zookeeper path to react to new/changed/removed jobs.
type Targeter struct {
	conn *zk.Conn
	path string

	children map[string]*PathTargeter
	once     sync.Once
	out      chan scraper.Job
}

// NewTargeter returns a new instance of Targeter.
func NewTargeter(config *TargeterConfig) (*Targeter, error) {
	t := &Targeter{
		conn: config.Conn,
		path: path.Join(config.Root, "scraper", config.Pool, "jobs"),

		children: map[string]*PathTargeter{},
		out:      make(chan scraper.Job),
	}
	go t.run()
	return t, nil
}

// TargeterConfig represents the configuration of a Targeter.
type TargeterConfig struct {
	Conn *zk.Conn
	Root string
	Pool string
}

// Targets implements scraper.Targeter interface.
// Returns a channel that feeds available jobs.
func (t *Targeter) Targets() <-chan scraper.Job {
	return t.out
}

func (t *Targeter) run() {
	defer close(t.out)
	log.WithField("path", t.path).Info("reading jobs from zookeeper")
	for {
		c, _, ech, err := t.conn.ChildrenW(t.path)
		if err != nil {
			log.WithError(err).WithField("path", t.path).Error("unable to get list of jobs from zookeeper")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}
		t.setChildren(c)
		<-ech
		log.WithFields(log.Fields{
			"path":     t.path,
			"num_jobs": len(c),
		}).Info("set jobs list")
	}
}

func (t *Targeter) setChildren(cn []string) {
	next := map[string]*PathTargeter{}
	for _, c := range cn {
		if pt, ok := t.children[c]; ok {
			next[c] = pt
			delete(t.children, c)
			continue
		}
		p := path.Join(t.path, c)
		pt := NewPathTargeter(&PathTargeterConfig{
			Conn: t.conn,
			Path: p,
		})
		next[c] = pt
		go func() {
			for j := range pt.Targets() {
				t.out <- j
			}
		}()
	}
	for _, pt := range t.children {
		pt.stop()
	}
	t.children = next
}
