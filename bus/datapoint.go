package bus

// Datapoint is 128 bits of time and a value
type Datapoint struct {
	Timestamp Timestamp
	Value     float64
}
