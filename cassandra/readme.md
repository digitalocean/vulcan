# Cassandra Resources

## Consulting Notes

* 200k column max per partition key (not the technical maximum but a rule of thumb max)
* Beware seconary indexes - understand how they're implemented if you really want to use them
* Beware "batch" writes, rather use a driver that is token aware (reads/writes directly to a node that owns the data) and round robins (shares the read/write load between replicas)
* Use prepared statements
* SSL between cassandra nodes has a large impact on performance
* Use `nodetool histograms` to check on table performance
* When you typically serve queries that read from more than 2 sstables, that's usually a problem
* Backfilling is not possible with DTCS, must insert data roughly in time order for sstable compaction to work correctly.
* Avoid gzip table compression, favor snappy and tune it to levels that make sense
* Enterprise in-memory table is sold by DataStax
* Look into cql tracing. At the driver you can specify to explain the query path and details. Many drivers you can configure cql tracing as a probability (e.g. trace 0.1% of queries)
* Any write creates data, even if you're overwriting the same value. Old values are tombstoned and cleaned up on memtable flush or sstable compaction.

## Online Resources

The SSTable File
http://www.scylladb.com/kb/sstable-data-file/

Compaction strategies
http://www.scylladb.com/kb/compaction/

DateTieredCompactionStrategy: Notes From the Field
http://www.datastax.com/dev/blog/dtcs-notes-from-the-field

Apache Cassandra Compaction Strategies
https://www.instaclustr.com/blog/2016/01/27/apache-cassandra-compaction/

Visualization on Compaction
https://labs.spotify.com/2014/12/18/date-tiered-compaction/

DateTieredCompactionStrategy: Compaction for Time Series Data
http://www.datastax.com/dev/blog/datetieredcompactionstrategy

Understanding the Impact of Cassandra Compact Storage
http://blog.librato.com/posts/cassandra-compact-storage