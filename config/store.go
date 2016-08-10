package config

// Store abstracts listing, setting, getting, and watching configuration changes
// in a centralized database
type Store interface {
	Delete(string) error
	List() ([]string, error)
	Set(string, []byte) error
	Get(string) ([]byte, error)
}
