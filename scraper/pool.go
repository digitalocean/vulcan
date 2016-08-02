package scraper

type Pool interface {
	Scrapers() <-chan []string
}
