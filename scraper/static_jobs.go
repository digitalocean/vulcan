package scraper

// StaticJob represents a vulcan job.
type StaticJob struct {
	jobName   JobName
	targeters []Targeter
}

// StaticJobConfig represents a StaticJob configuration.
type StaticJobConfig struct {
	JobName   JobName
	Targeters []Targeter
}

// NewStaticJob returns a new instance of the StaticJob configuration.
func NewStaticJob(config *StaticJobConfig) *StaticJob {
	return &StaticJob{
		jobName:   config.JobName,
		targeters: config.Targeters,
	}
}

// Name implements Job.
func (j *StaticJob) Name() JobName {
	return j.jobName
}

// GetTargets implements Job.
func (j *StaticJob) GetTargets() []Targeter {
	return j.targeters
}

// AddTargets appends the provided Targeters to the list of targets.
func (j *StaticJob) AddTargets(t ...Targeter) {
	j.targeters = append(j.targeters, t...)
}
