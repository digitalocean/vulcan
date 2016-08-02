package zookeeper

import (
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	pconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval/discovery/dns"
	"github.com/samuel/go-zookeeper/zk"
)

type PathTargeter struct {
	conn *zk.Conn
	path string

	done chan struct{}
	out  chan scraper.Job
	once sync.Once
}

type PathTargeterConfig struct {
	Conn *zk.Conn
	Path string
}

func NewPathTargeter(config *PathTargeterConfig) *PathTargeter {
	pt := &PathTargeter{
		conn: config.Conn,
		path: config.Path,

		done: make(chan struct{}),
		out:  make(chan scraper.Job),
	}
	go pt.run()
	return pt
}

func (pt PathTargeter) Targets() <-chan scraper.Job {
	return pt.out
}

func (pt PathTargeter) run() {
	defer close(pt.out)
	for {
		// escape
		select {
		case <-pt.done:
			return
		default:
		}

		log.WithField("path", pt.path).Info("getting value")
		b, _, ech, err := pt.conn.GetW(pt.path)
		if err != nil {
			log.WithError(err).Error("while getting path")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}
		jobs, err := pt.parseJobs(b)
		if err != nil {
			log.WithError(err).Error("while parsing value")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}
		for _, j := range jobs {
			pt.out <- j
		}

		// block
		select {
		case <-pt.done:
			return
		case <-ech:
		}
	}
}

func (pt PathTargeter) parseJobs(b []byte) ([]scraper.Job, error) {
	jobs := []scraper.Job{}
	c, err := pconfig.Load(string(b))
	if err != nil {
		return jobs, err
	}

	if len(c.ScrapeConfigs) < 1 {
		return jobs, errors.New("no scrape configs provided")
	}

	for _, sc := range c.ScrapeConfigs {
		j := scraper.Job{
			JobName: scraper.JobName(sc.JobName),
			Targets: map[scraper.Instance]scraper.Target{},
		}
		for _, tg := range sc.StaticConfigs {
			for _, t := range tg.Targets {
				inst := string(t[model.LabelName("__address__")])
				u, err := url.Parse(fmt.Sprintf("%s://%s%s", sc.Scheme, inst, sc.MetricsPath))
				if err != nil {
					log.WithError(err).Error("could not parse instance")
					continue
				}
				j.Targets[scraper.Instance(inst)] = scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
					Interval: time.Duration(sc.ScrapeInterval),
					URL:      *u,
				})
			}
		}
		jobs = append(jobs, j)

		dns.NewDiscovery(c.ScrapeConfigs[0].DNSSDConfigs[0])

	}
	return jobs, nil
}

func (pt PathTargeter) stop() {
	pt.once.Do(func() {
		close(pt.done)
		// drain
		for {
			select {
			case _, ok := <-pt.out:
				if !ok {
					return
				}
			}
		}
	})
}
