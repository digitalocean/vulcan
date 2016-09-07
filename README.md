# Vulcan [![Build Status](https://travis-ci.org/digitalocean/vulcan.svg?branch=master)](https://travis-ci.org/digitalocean/vulcan) [![Report Card](https://goreportcard.com/badge/github.com/digitalocean/vulcan)](https://goreportcard.com/report/github.com/digitalocean/vulcan)

Vulcan is an API-compatible extension to Prometheus. It aims to provide a better story for long-term storage, data durability, and high cardinality metrics while making sure that each component is horizontally scalable. Vulcan is much more complex to operate, but should integrate with ease to an existing Prometheus environment.

Vulcan is highly experimental.

## Contributing

Refer to [CONTRIBUTING.md](CONTRIBUTING.md)

## Contact

The core developers are accessible via the [Vulcan Developers Mailinglist](https://groups.google.com/forum/#!forum/vulcan-developers)

## Ethos

Vulcan should use open-source databases (e.g. Kafka, ElasticSearch, Cassandra)

Vulcan should strive to be API-compatible with Prometheus e.g. PromQL discussions and improvements should happen in the Prometheus community, committed in the Prometheus code base, and then used in Vulcan.

# Architecture

```
       ┌─────────────────────┐                                        
       │prometheus exporters ├─┐                                      
       └─┬───────────────────┘ ├─┐                                    
         └─┬───────────────────┘ │                                    
           └─────────────────────┘                                    
                      △                                               
                  ┌───┘                                               
                  ▼                                                   
       ┌─────────────────────┐                                        
       │     prometheus      │                                        
       │                     │                                        
       │(remote storage api) │                                        
       └─────────────────────┘                                        
                  │                                                   
                  ▼                                                   
       ┌─────────────────────┐                                        
       │      forwarder      │                      ┌────────────┐    
       └─────────────────────┘                    ┌─│downsamplers├─┐  
                  │                               │ └─┬──────────┘ ├─┐
                  └───────┐                       │   └─┬──────────┘ │
                          │                       │     └────────────┘
                          ▼                       ▼            ▲      
  ┌──── ───── ───── ───── ───── ───── ───── ───── ┐            │      
  │                     Kafka                     │◁───────────┘      
  └── ───── ───── ───── ───── ───── ───── ───── ──┘                   
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

### Forwarder [ NOT IMPLEMENTED ]

A forwarder implements the Prometheus remote storage API. A forwarder should live close to a Prometheus server (even on the same machine). The forwarder consumes metrics from Prometheus and writes them into Vulcan in a format that Vulcan expects.

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
