package scraper

// JobName is the name of an exporter/job.
type JobName string

// Job represents a discoverable targets to be be processed.
type Job interface {
	// TargetWatcher
	// Returns the unique name of a job.
	Name() JobName
	AddTargets(...Targeter)
	GetTargets() []Targeter
}
