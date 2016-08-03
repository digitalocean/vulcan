package bus

// AckPayload allows a SampleGroup to be passed around with a Done function so
// that a consumer can signal when the payload has been consumed.
type AckPayload struct {
	SampleGroup SampleGroup
	Done        func(error)
}

// AckSource proivdes a channel of AckPayload as a way to
// process SampleGroups. Stop can be called on the source to
// cause the channel to close. For each item in the payload, you
// MUST call Done. Done signifies that the contents of the
// payload have been processed, and takes an error parameter that may be
// nil. A non-nil value signifies that there is a non-
// recoverable error processing the payload. Calling Error on a payload
// item will cause the whole source to stop.
//
// The contents should be read until the channel
// is closed. The caller should call Err() after the channel is closed.
// Err() will return nil if the source closed without error, otherwise
// it will return the first error encountered.
type AckSource interface {
	Chan() <-chan AckPayload
	Err() error
	Stop()
}
