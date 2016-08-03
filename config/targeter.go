package config

type Targeter interface {
	Targets(done <-chan struct{}) (<-chan []Target, <-chan error)
}
