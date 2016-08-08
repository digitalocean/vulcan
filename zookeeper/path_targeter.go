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
)

// PathTargeter represents an object that connects to Zookeeper and queries
// a zk path for Vulcan scrape configurations for a specific zk path.
type PathTargeter struct {
	conn Client
	path string

	done chan struct{}
	out  chan []scraper.Job
	once sync.Once
}

// PathTargeterConfig represents the configuration of a PathTargeter.
type PathTargeterConfig struct {
	Conn Client
	Path string
}

// NewPathTargeter creates a new instance of PathTargeter.
func NewPathTargeter(config *PathTargeterConfig) *PathTargeter {
	pt := &PathTargeter{
		conn: config.Conn,
		path: config.Path,

		done: make(chan struct{}),
		out:  make(chan []scraper.Job),
	}
	go pt.run()
	return pt
}

// Jobs implements scraper.JobWatcher interface.
// Returns a channel that feeds available jobs.
func (pt *PathTargeter) Jobs() <-chan []scraper.Job {
	return pt.out
}

func (pt *PathTargeter) run() {
	defer close(pt.out)

	ll := log.WithFields(log.Fields{
		"path_targeter": "run",
		"path":          pt.path,
	})

	for {
		// escape the continue in the below if block
		select {
		case <-pt.done:
			return

		default:
		}

		ll.Info("getting value")
		b, _, ech, err := pt.conn.GetW(pt.path)
		if err != nil {
			ll.WithError(err).Error("while getting path")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}
		ll.Infof("got value:%q", string(b))

		jobs, err := pt.parseJobs(b)
		if err != nil {
			ll.WithError(err).Error("while parsing value")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}

		select {
		case <-pt.done:
			return

		case pt.out <- jobs:
		}

		select {
		case <-pt.done:
			return

		case <-ech:
		}
	}
}

func (pt *PathTargeter) parseJobs(b []byte) ([]scraper.Job, error) {
	jobs := []scraper.Job{}
	c, err := pconfig.Load(string(b))
	if err != nil {
		return jobs, err
	}

	if len(c.ScrapeConfigs) < 1 {
		return jobs, errors.New("no scrape configs provided")
	}

	for _, sc := range c.ScrapeConfigs {
		sj := pt.staticJobs(sc)
		log.WithField("path", pt.path).Debugf("parsed static jobs: %v", sj)
		// TODO handle other types of scrape configs (e.g. DNS and Kubernetes)
		jobs = append(jobs, sj)
	}
	return jobs, nil
}

func (pt *PathTargeter) staticJobs(sc *pconfig.ScrapeConfig) scraper.Job {
	j := scraper.NewStaticJob(&scraper.StaticJobConfig{
		JobName:   scraper.JobName(sc.JobName),
		Targeters: []scraper.Targeter{},
	})

	for _, tg := range sc.StaticConfigs {

		for _, t := range tg.Targets {
			inst := string(t[model.LabelName("__address__")])

			u, err := url.Parse(
				fmt.Sprintf("%s://%s%s", sc.Scheme, inst, sc.MetricsPath),
			)
			if err != nil {
				log.WithError(err).Error("could not parse instance")
				continue
			}

			j.AddTargets(scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
				Interval: time.Duration(sc.ScrapeInterval),
				URL:      *u,
				JobName:  scraper.JobName(sc.JobName),
			}))
		}

	}

	return j
}

func (pt *PathTargeter) stop() {
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
