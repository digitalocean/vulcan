package storage

import "github.com/digitalocean/vulcan/bus"

type MatchType int

const (
	Equal MatchType = iota
	NotEqual
	RegexpMatch
	RegexNoMatch
)

type Match struct {
	Type  MatchType
	Name  string
	Value string
}

// Resolver is a interface that wraps the Resolve method.
type Resolver interface {
	// Resolve makes a query using the provided key value pairs of query
	// params and transforms the results to Vulcan Metric type.
	// Resolve(map[string]string) ([]*bus.Metric, error)
	Resolve([]*Match) ([]*bus.Metric, error)
}
