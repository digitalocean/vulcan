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
	"github.com/prometheus/prometheus/retrieval"
	"golang.org/x/net/context"
)

const (
	staticJob     = `static`
	kubernetesJob = `kubernetes`
)

// PathTargeter represents an object that connects to Zookeeper and queries
// a zk path for Vulcan scrape configurations for a specific zk path.
type PathTargeter struct {
	conn Client
	path string
	jobs map[string]scraper.Job

	done chan struct{}
	out  chan []scraper.Job

	once  sync.Once
	mutex *sync.Mutex
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

		done:  make(chan struct{}),
		out:   make(chan []scraper.Job),
		mutex: new(sync.Mutex),
		// only support 2 job types ATM
		jobs: map[string]scraper.Job{},
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

		ll.Info("getting jobs")
		b, _, ech, err := pt.conn.GetW(pt.path)
		if err != nil {
			ll.WithError(err).Error("while getting path")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}
		ll.WithField("config", string(b)).Info("got job")

		ctx, cancelFunc := context.WithCancel(context.Background())

		scfgs, err := pt.parseConfig(b)
		if err != nil {
			ll.WithError(err).Error("while parsing job config")
			time.Sleep(time.Second * 2) // TODO exponential backoff
			continue
		}

		for i, sc := range scfgs {
			// static configs
			pt.setJob(fmt.Sprintf("%s/%d", staticJob, i), sc.StaticConfigs, sc)
			ll.WithField("jobs", pt.jobs).Info("parsed static jobs")

			// K8 service discovery configs
			pt.k8Jobs(sc, k8discovery, ctx)
			ll.WithField("jobs", pt.jobs).Info("parsed kubernetes jobs")

		}
		// block until job slice sent
		select {
		case <-pt.done:
			cancelFunc()
			return

		case pt.out <- pt.allJobs():
		}

		select {
		case <-pt.done:
			cancelFunc()
			return

		case <-ech:
			break
		}

	}
}

func (pt *PathTargeter) parseConfig(b []byte) ([]*pconfig.ScrapeConfig, error) {
	c, err := pconfig.Load(string(b))
	if err != nil {
		return nil, err
	}

	if len(c.ScrapeConfigs) < 1 {
		return nil, errors.New("no scrape configs provided")
	}

	return c.ScrapeConfigs, nil
}

func (pt *PathTargeter) k8Jobs(
	sc *pconfig.ScrapeConfig,
	providerFn func(*pconfig.KubernetesSDConfig) (retrieval.TargetProvider, error),
	ctx context.Context,
) {
	var (
		ch = make(chan []*pconfig.TargetGroup)
		wg = new(sync.WaitGroup)
		ll = log.WithFields(log.Fields{
			"path": pt.path,
			"type": kubernetesJob,
		})
	)

	for i, config := range sc.KubernetesSDConfigs {
		ll = ll.WithField("config", *config)
		ll.Debug("processing k8 config")

		kd, err := providerFn(config)
		if err != nil {
			ll.WithError(err).Error("could not create discovery object")
			continue
		}
		go pt.discoverTargetGroups(
			kd,
			fmt.Sprintf("%s/%d", kubernetesJob, i),
			sc,
			wg,
			ch,
			ctx,
			ll,
		)
	}
	// wait for initial target groups to be recieved
	wg.Wait()

}

func (pt *PathTargeter) discoverTargetGroups(
	provider retrieval.TargetProvider,
	jobKey string,
	sc *pconfig.ScrapeConfig,
	wg *sync.WaitGroup,
	ch chan []*pconfig.TargetGroup,
	ctx context.Context,
	ll *log.Entry,
) {
	// Block until initial set of configuations is got
	wg.Add(1)

	go provider.Run(ctx, ch)

	ll.Debug("waiting for initial discovery target groups")
	select {
	case chTgs := <-ch:
		ll.WithField("root_key", jobKey).Debug("initial discovery target groups received")
		pt.setJob(jobKey, chTgs, sc)

	case <-ctx.Done():
		return
	}

	wg.Done()

	// Listen for updates and send new job list to out channel
	for {
		select {
		case chTgs := <-ch:
			ll.WithFields(log.Fields{
				"root_key":         jobKey,
				"new_targetgroups": chTgs,
			}).Debug("update discovery target groups received")

			pt.setJob(jobKey, chTgs, sc)
			pt.out <- pt.allJobs()

			ll.Debug("update discovery jobs sent")

		case <-ctx.Done():
			return
		}
	}
}

func (pt *PathTargeter) tgToJob(tg *pconfig.TargetGroup, sc *pconfig.ScrapeConfig) scraper.Job {
	var (
		ll = log.WithFields(log.Fields{
			"path_targeter": "tgToJob",
			"path":          pt.path,
			"source":        tg.Source,
		})
		j = scraper.NewStaticJob(&scraper.StaticJobConfig{
			JobName:   scraper.JobName(sc.JobName),
			Targeters: []scraper.Targeter{},
		})
	)

	ll.Debug("converting target groups")

	for _, t := range tg.Targets {

		labelAddr, ok := t[model.LabelName("__address__")]
		if !ok {
			ll.WithField("target", t.String()).Error("__address__ label key not found")
			continue
		}

		inst := string(labelAddr)

		u, err := url.Parse(
			fmt.Sprintf("%s://%s%s", sc.Scheme, inst, sc.MetricsPath),
		)
		ll.WithField("target_url", u).Debug("converting target")
		if err != nil {
			log.WithError(err).Error("could not parse instance")
			continue
		}

		j.AddTargets(scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
			Interval: time.Duration(sc.ScrapeInterval),
			URL:      u,
			JobName:  scraper.JobName(sc.JobName),
		}))
	}

	return j
}

func (pt *PathTargeter) setJob(key string, tgs []*pconfig.TargetGroup, sc *pconfig.ScrapeConfig) {
	ll := log.WithFields(log.Fields{
		"path_targeter":      "setJob",
		"path":               pt.path,
		"root_key":           key,
		"target_group_count": len(tgs),
	})

	for _, tg := range tgs {
		if tg == nil {
			ll.Error("got nil target group")
			continue
		}

		pt.mutex.Lock()
		ll.WithFields(log.Fields{
			"source":  tg.Source,
			"targets": tg.Targets,
		}).Debug("converting targets")

		jobKey := fmt.Sprintf("%s/%s", key, tg.Source)
		pt.jobs[jobKey] = pt.tgToJob(tg, sc)
		ll.WithFields(log.Fields{
			"job_key":      jobKey,
			"current_jobs": pt.jobs,
		}).Debug("job set")

		pt.mutex.Unlock()
	}
}

func (pt *PathTargeter) allJobs() []scraper.Job {
	var (
		jobs = []scraper.Job{}
		ll   = log.WithFields(log.Fields{
			"path_targeter": "allJobs",
			"path":          pt.path,
		})
	)

	pt.mutex.Lock()

	ll.WithField("current_jobs", pt.jobs).Debug("preparing job slice from current jobs")
	for _, job := range pt.jobs {
		jobs = append(jobs, job)
	}
	ll.WithFields(log.Fields{
		"current_jobs": pt.jobs,
		"job_slice":    jobs,
	}).Debug("job slice prepared")

	pt.mutex.Unlock()

	return jobs
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
