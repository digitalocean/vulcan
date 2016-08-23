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
