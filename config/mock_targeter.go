package config

type mockTargeter struct {
	tlch  chan []Target
	errch chan error
}

func NewMockTargeter() *mockTargeter {
	return &mockTargeter{
		tlch:  make(chan []Target),
		errch: make(chan error),
	}
}

func (mt mockTargeter) Targets(done <-chan struct{}) (<-chan []Target, <-chan error) {
	return mt.tlch, mt.errch
}

func (mt mockTargeter) Send(targets []Target) {
	mt.tlch <- targets
}
