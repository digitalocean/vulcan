package scraper

// Pool is an interface that wraps the Scrapers method.
type Pool interface {
	Scrapers() <-chan []string
}
