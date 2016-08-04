/*
Package bus defines interfaces for writing to and reading from the vulcan metric
bus and the datastructures that get passed. Different packages (e.g. kafka)
should actually implement the bus interfaces.
*/
package bus

// TODO scraper.Writer interface should live in bus package
// TODO Metrics and Samples are a concept that exist in all layers of vulcan,
// not just the bus. These structs should likely be in a more fundamental
// package.
