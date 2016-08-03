package bus

// Metric must have a name and has 0..many labels. Modeled after prometheus
// metrics.
// TODO each prometheus metric also has a type (counter, gauge...) we should
// probably make this a field in the Metric struct. Right now the type is
// recorded as a label named "__type__".
type Metric struct {
	Name   string
	Labels map[string]string
}
