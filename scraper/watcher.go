package scraper

// TargetWatcher is an interface that represents something that can return
// a slice of active Targeters
type TargetWatcher interface {
	Targets() <-chan []Targeter
}

// JobWatcher is an interface that wraps the Target method.
type JobWatcher interface {
	Jobs() <-chan []Job
}
