package config

import "time"

type Target struct {
	Job      string
	URL      string
	Instance string
	Interval time.Duration
}

func (t Target) Key() string {
	return t.Job + t.URL
}
