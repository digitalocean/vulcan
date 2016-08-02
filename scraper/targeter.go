package scraper

type JobName string

type Instance string

type Job struct {
	JobName JobName
	Targets map[Instance]Target
}

type Targeter interface {
	Targets() <-chan Job
}
