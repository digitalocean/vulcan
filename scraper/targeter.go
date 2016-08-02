package scraper

// JobName is the name of an exporter/job.
type JobName string

// Instance is a target host to scrape.
type Instance string

// Job represents a vulcan job.
type Job struct {
	JobName JobName
	Targets map[Instance]Target
}

// Targeter is an interface that wraps the Target method.
type Targeter interface {
	Targets() <-chan Job
}
