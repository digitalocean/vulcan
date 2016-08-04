package bus

// Sample is a metric and a single associated datapoint
type Sample struct {
	Metric    Metric
	Datapoint Datapoint
}
