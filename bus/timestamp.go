package bus

// Timestamp is the milliseconds since the unix epoch. We use this instead of go's time.Time
// since prometheus expects time to be represented in this way, and this value is also more
// natural for our data model.
//
// converting to a go time.Time value is simple: `time.Unix(t/1000, (ts%1000)*1000*1000))`
// but does cost extra instructions when we do this for EVERY datapoint only to turn around
// and turn it back into a int64 for prometheus query engine. Plus, dividing is an expensive
// instruction.
type Timestamp int64
