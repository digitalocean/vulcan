# scraper

The scraper is responsible for polling prometheus exporters and writing metrics
onto the metric bus. Jobs are defined in a configuration store and a pool of
scrapers share the load to fulfill these jobs. Scrapers react to other scrapers
joining or leaving the pool. Scrapers react to jobs changing.

## scraper architecture

```
 ╔═════════════════════════════════════════════════════════════════════════════╗
 ║                       configuration store (zookeeper)                       ║
 ║  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓  ║
 ║  ┃               jobs                ┃ ┃         alive scrapers          ┃  ║
 ║  ┃┌─────────────────────────────────┐┃ ┃┌───────────────────────────────┐┃  ║
 ║  ┃│job_name: mysql                  │┃ ┃│scraper-ff55531                │┃  ║
 ║  ┃│static_configs:                  │┃ ┃│scraper-e85aae3                │┃  ║
 ║  ┃│  - targets:                     │┃ ┃│scraper-9c2ff7b                │┃  ║
 ║  ┃│    - mysql1.example.com:9104    │┃ ┃└───────────────────────────────┘┃  ║
 ║  ┃│    - mysql2.example.com:9104    │┃ ┃                                 ┃  ║
 ║  ┃└─────────────────────────────────┘┃ ┃                                 ┃  ║
 ║  ┃┌─────────────────────────────────┐┃ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛  ║
 ║  ┃│job_name: memcached              │┃                  △                   ║
 ║  ┃│dns_sd_configs:                  │┃                  │                   ║
 ║  ┃│  - names:                       │┃                  │                   ║
 ║  ┃│    - _memcached._tcp.example.com│┃                  │                   ║
 ║  ┃└─────────────────────────────────┘┃                  │                   ║
 ║  ┃┌─────────────────────────────────┐┃                  │                   ║
 ║  ┃│job_name: nginx                  │┃                  │                   ║
 ║  ┃│consul_sd_configs:               │┃                  │                   ║
 ║  ┃│  - server: 'consul:1234'        │┃                  │                   ║
 ║  ┃│    services: ['nginx']          │┃                ┌─┘                   ║
 ║  ┃└─────────────────────────────────┘┃                │                     ║
 ║  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛                │                     ║
 ║                    △                                  │                     ║
 ║                    │                                  │                     ║
 ╚════════════════════╬══════════════════════════════════╬═════════════════════╝
                    ┌─┘                                  │
 ╔══════════════════╬════════════════════════════════════╬═════════════════════╗
 ║                  │           each scraper node        │                     ║
 ║                  │                                    │                     ║
 ║                  ▼                                    ▼                     ║
 ║  ┌───────────────────────────────┐    ┌───────────────────────────────┐     ║
 ║  │        resolve targets        │    │        consistent hash        │     ║
 ║  │                               │ ┌─▶│                               │     ║
 ║  │┌─────────────────────────────┐│ │  └───────────────────────────────┘     ║
 ║  ││                             ││ │                  │                     ║
 ║  ││                             ││ │                  ▼                     ║
 ║  ││mysql1.example.com:9104      ││ │  ┌───────────────────────────────┐     ║
 ║  ││mysql2.example.com:9104      ││ │  │          my targets           │     ║
 ║  ││memcached1.example.com:9106  ││ │  │                               │     ║
 ║  ││memcached2.example.com:9106  ││─┘  │┌─────────────────────────────┐│     ║
 ║  ││memcached3.example.com:9106  ││    ││                             ││     ║
 ║  ││nginx1.example.com:9113      ││    ││                             ││     ║
 ║  ││nginx2.example.com:9113      ││    ││mysql1.example.com:9108      ││     ║
 ║  ││                             ││    ││memcached3.example.com:9109  ││     ║
 ║  ││                             ││    ││                             ││     ║
 ║  │└─────────────────────────────┘│    ││                             ││     ║
 ║  │                               │    │└─────────────────────────────┘│     ║
 ║  └───────────────────────────────┘    └───────────────────────────────┘     ║
 ║                                                                             ║
 ╚═════════════════════════════════════════════════════════════════════════════╝
 ▲                                                                             │
 │        ┌───────────────────────────┐                        ┌───────────────┘
 ├───────▷│  mysql1.example.com:9108  │                        │
 │        └───────────────────────────┘                        ▼
 │        ┌───────────────────────────┐        ┌───────────────────────────────┐
 └───────▷│memcached3.example.com:9109│        │      metric bus (kafka)       │
          └───────────────────────────┘        └───────────────────────────────┘
```          