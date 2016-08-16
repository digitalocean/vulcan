package storage

import "github.com/digitalocean/vulcan/bus"

// MatchType is an enum representing a PromQL label match from "=", "!=", "=~"
// or "!~"
type MatchType int

const (
	// Equal is "=" in PromQL
	Equal MatchType = iota
	// NotEqual is "!=" in PromQL
	NotEqual
	// RegexMatch is "=~" in PromQL
	RegexMatch
	// RegexNoMatch is "!~" in PromQL
	RegexNoMatch
)

// Match represents the PromQL matching function and on what label name to
// operate.
type Match struct {
	Type  MatchType
	Name  string
	Value string
}

// Resolver is a interface that wraps the Resolve method.
type Resolver interface {
	// Resolve makes a query using the provided key value pairs of query
	// params and transforms the results to Vulcan Metric type.
	Resolve([]*Match) ([]*bus.Metric, error)
}
