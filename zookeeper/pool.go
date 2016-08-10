package zookeeper

import (
	"path"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

// Pool uses zookeeper as a backend to register a scraper's existence and watches
// zookeeper for changes in the list of active scrapers.
type Pool struct {
	id   string
	conn Client
	path string
	done chan struct{}
	out  chan []string
	once sync.Once
}

// NewPool returns a new instance of Pool.
func NewPool(config *PoolConfig) (*Pool, error) {
	p := &Pool{
		id:   config.ID,
		conn: config.Conn,
		path: path.Join(config.Root, "scraper", "scrapers"),
		done: make(chan struct{}),
		out:  make(chan []string),
	}
	go p.run()
	return p, nil
}

// PoolConfig represents the configuration of a Pool object.
type PoolConfig struct {
	ID   string
	Conn Client
	Root string
}

func (p *Pool) run() {
	defer close(p.out)
	mypath := path.Join(p.path, p.id)
	mylog := log.WithFields(log.Fields{
		"path": p.path,
		"id":   p.id,
	})
	// ensure path exists
	parts := strings.Split(mypath, "/")
	acc := ""
	for i := 1; i < len(parts)-1; i++ {
		acc = acc + "/" + parts[i]
		log.WithFields(log.Fields{
			"path": acc,
		}).Debug("ensuring path exists")
		exists, _, err := p.conn.Exists(acc)
		if err != nil {
			log.Fatal(err)
		}
		if exists {
			log.WithFields(log.Fields{
				"path": acc,
			}).Debug("path already exists")
			continue
		}
		_, err = p.conn.Create(acc, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Fatal(err)
		}
	}
	mylog.Info("registering self in zookeeper")
	_, err := p.conn.Create(mypath, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		mylog.WithError(err).Error("could not register self in pool")
		return
	}
	for {
		// escape
		select {
		case <-p.done:
			return
		default:
		}

		ch, _, ech, err := p.conn.ChildrenW(p.path)
		if err != nil {
			mylog.WithError(err).Error("error while getting active scraper list from zookeeper")
			time.Sleep(time.Second * 2) // TODO backoff and report error
			continue
		}
		mylog.WithFields(log.Fields{
			"scrapers":     ch,
			"num_scrapers": len(ch),
		}).Info("got list of scrapers from zookeeper")

		p.out <- ch

		// block
		select {
		case <-p.done:
			return
		case <-ech:
		}
	}
}

// Stop signals the current Pool instance to stop running.
func (p *Pool) Stop() {
	p.once.Do(func() {
		close(p.done)
	})
}

// Scrapers returns a channel that sends a slice of active Scraper instances.
func (p *Pool) Scrapers() <-chan []string {
	return p.out
}
