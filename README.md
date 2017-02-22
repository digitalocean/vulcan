# Vulcan [![Build Status](https://travis-ci.org/digitalocean/vulcan.svg?branch=master)](https://travis-ci.org/digitalocean/vulcan) [![Report Card](https://goreportcard.com/badge/github.com/digitalocean/vulcan)](https://goreportcard.com/report/github.com/digitalocean/vulcan)

Vulcan extends Prometheus adding horizontal scalability and long-term storage.

_Vulcan is highly experimental._

## Why

Prometheus has an upper-limit on the number of samples it can handle and manually sharding Prometheus is difficult. Prometheus provides
no built-in way to rebalance data between nodes once sharded, which makes accommodating additional load via adding nodes a difficult, manual process. Queries
against manually-sharded Prometheus servers must be rethought since each Prometheus instance only has a subset of the total metrics.

It is difficult to retain data in Prometheus for long-term storage as there is no built-in way to backup and restore Prometheus data. Mirroring
Prometheus (running multiple identically-configured Prometheus servers) is an option for high availability (and good for the role of monitoring),
but newly created mirrors lack historical data and therefore don't provide historical data or any additional replication factor.

Vulcan is horizontally scalable and built for long-term storage. In order to accommodate growing load, add more resources to Vulcan. There is no need to think about how to shard
 data and how sharding will affect queries.

Prometheus (as of v1.2.1) is able to forward metrics to Vulcan. Existing Prometheus deployments can easily reconfigure their Prometheus servers to forward all (or just some) metrics
to Vulcan. Prometheus can continue operating as a simple and reliable monitoring system while utilizing Vulcan for long-term storage.

### Why the name Vulcan?

_Vulcan is the roman god of fire, metalworking and of the forge. Raised in the [digital] ocean, Vulcan was charged with crafting the tools and weaponry._

Vulcan aims to enhance the Prometheus ecosystem. Thank you Prometheus for stealing us fire in the first place.

## Architecture

Refer to [architecture.md](architecture.md)

## Contributing

Refer to [CONTRIBUTING.md](CONTRIBUTING.md)

## Contact

The core developers are accessible via the [Vulcan Developers Mailinglist](https://groups.google.com/forum/#!forum/vulcan-developers)

## Ethos

Vulcan components should be stateless; state should be handled by open-source databases (e.g. Cassandra, Kafka).

Vulcan should be API-compatible with Prometheus. e.g. PromQL discussions and improvements should happen in the 
Prometheus community, committed to Prometheus, and then utilized in Vulcan.

## License

Apache License 2.0, see [LICENSE](LICENSE).
