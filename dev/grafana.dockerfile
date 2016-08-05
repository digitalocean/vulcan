FROM grafana/grafana:3.1.1

RUN apt-get update \
 && apt-get install -y curl \
 && apt-get clean \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*
