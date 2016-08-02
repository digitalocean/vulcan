# Dev

Greetings developer! Vulcan is complicated to run and requires running multiple
workers and multiple databases. We use Docker Compose to make it easier to run
Vulcan locally.

# Docker Compose

You will need [Docker Compose](https://docs.docker.com/compose/) installed on your local machine.

Once installed, you can run `docker-compose up -d` in this directory to start
kafka, zookeeper, cassandra, and elasticsearch along with vulcan workers. All
these services are configured in the `docker-compose.yml` file.

`docker-compose ps` will list the services and their state (Up/Exited).

`docker-compose logs {service name}` will show the logs for a given service.

`docker-compose build` will compile vulcan and create a new docker image.

For debugging Cassandra, you can run `./cqlsh` which will attach your terminal to
a cqlsh command running in the docker-compose network.


