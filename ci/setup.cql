CREATE KEYSPACE IF NOT EXISTS ci_vulcan
WITH REPLICATION = {
  'class':              'SimpleStrategy',
  'replication_factor': 1
}
AND DURABLE_WRITES = true;

-- https://github.com/jeffjirsa/twcs
-- You should target fewer than 50 buckets per table based on your TTL. A TTL of 
-- 90 days may want 3 day buckets, where a TTL of 2 hours may want 5 minute buckets.

CREATE TABLE IF NOT EXISTS ci_vulcan.varbit (
  id text,
  start bigint,
  end bigint,
  chunk blob,
  PRIMARY KEY (id, start)
) WITH CLUSTERING ORDER BY (start ASC)
  AND COMPACTION = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': '12'
  } 
  AND COMPRESSION = {
    'sstable_compression': ''
  };
