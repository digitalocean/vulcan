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
