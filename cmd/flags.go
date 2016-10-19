// Copyright 2016 The Vulcan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

const (
	magicPageSize = 120 // about 30 minutes of datapoints at 15s resolution = 30min * 60 seconds/min * 1 datapoint/15seconds
	magicPrefetch = 1.5 // should always have next page ready to go, and half-way through current page start getting the next-next page
)

// Vulcan command line flag names.
const (
	flagAddress               = "address"
	flagCassandraAddrs        = "cassandra-addrs"
	flagCassandraKeyspace     = "cassandra-keyspace"
	flagCassandraPageSize     = "cassandra-page-size"
	flagCassandraPrefetch     = "cassandra-prefetch"
	flagCassandraTimeout      = "cassandra-timeout"
	flagCassandraNumConns     = "cassandra-num-conns"
	flagESAddrs               = "es"
	flagESIndex               = "es-index"
	flagESSniff               = "es-sniff"
	flagKafkaAddrs            = "kafka-addrs"
	flagKafkaClientID         = "kafka-client-id"
	flagKafkaGroupID          = "kafka-group-id"
	flagKafkaTopic            = "kafka-topic"
	flagKafkaBatchSize        = "kafka-batch-size"
	flagNumCassandraWorkers   = "num-cassandra-workers"
	flagNumKafkaWorkers       = "num-kafka-workers"
	flagNumWorkers            = "num-workers"
	flagTelemetryPath         = "telemetry-path"
	flagWebListenAddress      = "web-listen-address"
	flagKafkaTrackWrites      = "kafka-track-writes"
	flagUncompressedTTL       = "uncompressed-ttl"
	flagDownsamplerResolution = "resolution"
	flagDownsampledTTL        = "downsampler-ttl"
)
