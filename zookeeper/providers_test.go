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
	"time"

	pconfig "github.com/prometheus/prometheus/config"
	"golang.org/x/net/context"
)

type MockTargetProvider struct {
	Interval time.Duration

	ctx context.Context
	out chan<- []*pconfig.TargetGroup
}

func (tp *MockTargetProvider) Run(ctx context.Context, up chan<- []*pconfig.TargetGroup) {
	tp.ctx = ctx
	tp.out = up
}

func (tp *MockTargetProvider) SendTargetGroups(tg []*pconfig.TargetGroup) {
	time.Sleep(tp.Interval)

	select {
	case <-tp.ctx.Done():
		return

	case tp.out <- tg:
	}
}
