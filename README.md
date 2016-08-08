# Vulcan

Vulcan is an API-compatible alternative to Prometheus. It aims to provide a better story for long-term storage, data durability, high cardinality metrics, high availability, and scalability. Vulcan is much more complex to operate, but should integrate with ease to an existing Prometheus environment. Vulcan aims to be familiar to Prometheus with configuration and queries by understanding the Prometheus scrape configuration file and the Prometheus Query Language.

## Ethos

Vulcan should use open-source databases (e.g. Kafka, ElasticSearch, Cassandra)

Vulcan should strive to be API-compatible with Prometheus. E.g. additional scraper job service discovery should be committed upstream to Prometheus then included in Vulcan. E.g. PromQL discussions and improvements should happen in the Prometheus community.

# Architecture

```
       ┌─────────────────────┐
       │prometheus exporters ├─┐
       └─┬───────────────────┘ ├─┐
         └─┬───────────────────┘ │
           └─────────────────────┘
                      △
                      │
                      │
                      ▼
             ┌─────────────────┐
             │    scrapers     ├─┐
             └─┬───────────────┘ ├─┐                ┌────────────┐
               └─┬───────────────┘ │              ┌─│downsamplers├─┐
                 └─────────────────┘              │ └─┬──────────┘ ├─┐
                          │                       │   └─┬──────────┘ │
                          │                       │     └────────────┘
                          ▼                       ▼            ▲
  ┌───────────────────────────────────────────────┐            │
  │                     KAFKA                     │────────────┘
  └───────────────────────────────────────────────┘
                          △
         ┌────────────────┴─┬────────────────────┐
         │                  │                    │
         ▼                  ▼                    ▼
  ┌────────────┐     ┌─────────────┐      ┌─────────────┐
  │  indexers  ├─┐   │  ingesters  ├─┐    │ compactors  ├─┐
  └─┬──────────┘ ├─┐ └─┬───────────┘ ├─┐  └─┬───────────┘ ├─┐
    └─┬──────────┘ │   └─┬───────────┘ │    └─┬───────────┘ │
      └────────────┘     └─────────────┘      └─────────────┘
             │                  │                    │
            ┌┘                  └─┬──────────────────┘
            │                     │
            ▼                     ▼
   ┌──── ───── ───── ┐    ┌──── ───── ────┐
                     │    │
   │                 │    │               │
   │  ElasticSearch  │        Cassandra   │
   │                 │    │               │
   │                      │               │
   └ ───── ───── ────┘    └── ───── ───── ┘
            △                     △
            └──┬──────────────────┘
               │
               ▼
         ┌──────────┐                       ┌───────────┐
         │ queriers ├─┐                     │  PromQL   │
         └─┬────────┘ ├─┐         ┌────────▶│  grafana  │
           └─┬────────┘ │◁────────┘         └───────────┘
             └──────────┘
```

## Components

### Scrapers

Scrapers put metrics onto the Vulcan metrics bus from prometheus exporters. They are a pool of workers that coordinate to share the load of polling all configured prometheus exporters and writing the polled metrics onto the bus (Kafka). Scrapers are configured in a familiar way to configuring the scrape jobs of a Prometheus server.

### Downsamplers [ NOT IMPLEMENTED ]

Downsamplers put lower resolution metrics onto the Vulcan metrics bus. They are a pool of workers consuming the metrics bus populated by the scrapers and writing their lower resolution version of those same metrics back onto the bus (but in a different topic). Lower resolution data can be more easily retained for long periods of time. Downsamplers are configurable to a target resolution. This allows a scraper configured for a high-frequency 15s resolution to passively also write metrics at a lower 10m resolution.

### Indexers

Indexers are a pool of workers that consume the metrics bus into a search database. Indexers do not care about the datapoints, rather, just what metrics exist. The search database is utilized by the querier in order to support the Prometheus Query Language.

### Ingesters

Ingesters are a pool of workers that consume the metrics bus into a datapoint database. The datapoints recorded should be queryable as soon as they are written. Ingesters read both the scraper's topic and the downsampler's topic into separate databases. The querier can decide based on the Prometheus QueryL what resolution database is most appropriate to fulfil the query.

### Compactors [ NOT IMPLEMENTED ]

Compactors are a pool of workers that consume the metrics bus into a datapoint database. Unlike the ingesters, the compactor does not immediately make metrics from the bus queryable. The compactor waits for a configurable time (e.g. 2 hours) per metric so that datapoints can be compressed and written into the datapoint database in a much more compact format. The querier attempts to fulfil a query from compacted data first, and then uncompressed data written by the ingesters. Compactors read both the scraper's topic and the downsampler's topic into separate databases.

### Queriers

Queriers are a pool web nodes that provide the Prometheus HTTP query API. The querier parses PromQL and queries the appropriate databases to get the raw datapoints and evalutates the PromQL functions on the data.

## License

Apache License 2.0, see [LICENSE](LICENSE).
