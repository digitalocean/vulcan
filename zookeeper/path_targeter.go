// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zookeeper

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/digitalocean/vulcan/scraper"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	pconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/util/httputil"
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
		ll = ll.WithField("kubernetes_config", *config)
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
		err error
	)

	ll.Debug("converting target groups")
	for _, t := range tg.Targets {
		t = pt.applyDefaultLabels(t, tg.Labels, sc)

		if len(sc.RelabelConfigs) > 0 && sc.RelabelConfigs[0] != nil {
			ll.Debug("relabel configs received")

			t, err = retrieval.Relabel(t, sc.RelabelConfigs...)
			if err != nil {
				ll.WithError(err).Error("could not relabel")
				continue
			}
			ll.WithField("labels", t).Warn("relabelled configs")
		}

		u, err := pt.getTargetURL(t, sc.MetricsPath, sc.Params)
		if err != nil {
			ll.WithError(err).Error("target invaldated")
			continue
		}

		ll.WithField("target_url", u).Debug("converting target")

		client, err := newHTTPClient(sc)
		if err != nil {
			ll.WithError(err).Error("could create http client for target")
			continue
		}

		j.AddTargets(scraper.NewHTTPTarget(&scraper.HTTPTargetConfig{
			Interval: time.Duration(sc.ScrapeInterval),
			URL:      u,
			JobName:  scraper.JobName(sc.JobName),
			Client:   client,
		}))
	}

	return j
}

func (pt *PathTargeter) applyDefaultLabels(
	target model.LabelSet,
	tgLabels model.LabelSet,
	sc *pconfig.ScrapeConfig,
) model.LabelSet {
	// initial labels, will be overwritten by higher priority labels
	iLabels := model.LabelSet{
		model.SchemeLabel:      model.LabelValue(sc.Scheme),
		model.MetricsPathLabel: model.LabelValue(sc.MetricsPath),
		model.JobLabel:         model.LabelValue(sc.JobName),
	}

	// labelize params in case in case needed for relabelling
	for k, v := range sc.Params {
		if len(v) > 0 {
			iLabels[model.LabelName(model.ParamLabelPrefix+k)] = model.LabelValue(v[0])
		}
	}

	// apply common target group labels
	for k, v := range tgLabels {
		iLabels[k] = v
	}

	// apply target labels
	for k, v := range target {
		iLabels[k] = v
	}

	return iLabels
}

func (pt *PathTargeter) getTargetURL(
	target model.LabelSet,
	defaultMetricsPath string,
	params url.Values,
) (*url.URL, error) {
	var (
		metricsPath string
		scheme      string
	)

	host, ok := target[model.AddressLabel]
	if !ok {
		return nil, errors.New("__address__ label key not found")
	}

	if _, ok = target[model.MetricsPathLabel]; !ok {
		metricsPath = defaultMetricsPath
	} else {
		metricsPath = string(target[model.MetricsPathLabel])
	}

	switch target[model.SchemeLabel] {
	case "http", "":
		scheme = "http"

	case "https":
		scheme = "https"

	default:
		return nil, errors.Errorf("invalid url scheme: %v", target[model.SchemeLabel])
	}

	// check for additional params
	for k, v := range target {
		if strings.HasPrefix(string(k), model.ParamLabelPrefix) {
			// override original param if found in target labels
			params[string(k[len(model.ParamLabelPrefix):])] = []string{string(v)}
		}
	}

	return &url.URL{
		Scheme:   scheme,
		Host:     string(host),
		Path:     metricsPath,
		RawQuery: params.Encode(),
	}, nil
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

// newHTTPClient returns a new http client.  Taken directly from prometheus.
func newHTTPClient(sc *pconfig.ScrapeConfig) (*http.Client, error) {
	tlsOpts := httputil.TLSOptions{
		InsecureSkipVerify: sc.TLSConfig.InsecureSkipVerify,
		CAFile:             sc.TLSConfig.CAFile,
	}
	if len(sc.TLSConfig.CertFile) > 0 && len(sc.TLSConfig.KeyFile) > 0 {
		tlsOpts.CertFile = sc.TLSConfig.CertFile
		tlsOpts.KeyFile = sc.TLSConfig.KeyFile
	}
	if len(sc.TLSConfig.ServerName) > 0 {
		tlsOpts.ServerName = sc.TLSConfig.ServerName
	}
	tlsConfig, err := httputil.NewTLSConfig(tlsOpts)
	if err != nil {
		return nil, err
	}
	// The only timeout we care about is the configured scrape timeout.
	// It is applied on request. So we leave out any timings here.
	var rt http.RoundTripper = &http.Transport{
		Proxy:             http.ProxyURL(sc.ProxyURL.URL),
		DisableKeepAlives: true,
		TLSClientConfig:   tlsConfig,
	}

	// If a bearer token is provided, create a round tripper that will set the
	// Authorization header correctly on each request.
	bearerToken := sc.BearerToken
	if len(bearerToken) == 0 && len(sc.BearerTokenFile) > 0 {
		b, err := ioutil.ReadFile(sc.BearerTokenFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read bearer token file %s: %s", sc.BearerTokenFile, err)
		}
		bearerToken = string(b)
	}

	if len(bearerToken) > 0 {
		rt = httputil.NewBearerAuthRoundTripper(bearerToken, rt)
	}

	if sc.BasicAuth != nil {
		rt = httputil.NewBasicAuthRoundTripper(sc.BasicAuth.Username, sc.BasicAuth.Password, rt)
	}

	// Return a new client with the configured round tripper.
	return httputil.NewClient(rt), nil
}
