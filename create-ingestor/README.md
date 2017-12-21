# create-ingestor

## Prerequisites

* Python3

* Python dependencies listed in [requirements.txt](./requirements.txt)

## Quick Start Guide

See [quickstart.md](./quickstart.md)

## Usage

```bash

usage: create-ingestor [-h] [-d] [-g] [-i INGEST_FILE] [-c CASSANDRA_HOST]
                       [-cp CASSANDRA_PORT] -u CASSANDRA_USERNAME -p
                       CASSANDRA_PASSWORD [-pk PARTITION_KEY]
                       [-ck CLUSTER_KEY]
                       avro_schema_file keyspace table

creates a Kafka ingestion file source for a Cassandra table, creating the table if it does not yet exist.

positional arguments:
  avro_schema_file      Avro schema file
  keyspace              Cassandra keyspace to ingest into
  table                 Cassandra table to ingest into

optional arguments:
  -h, --help            show this help message and exit
  -d, --dry             dry run
  -g, --debug           debug this script
  -i INGEST_FILE, --ingest-file INGEST_FILE
                        file to use as ingestion point, defaults to <table>.kafka
  -c CASSANDRA_HOST, --cassandra-host CASSANDRA_HOST
                        Cassandra host to connect to (default localhost)
  -cp CASSANDRA_PORT, --cassandra-port CASSANDRA_PORT
                        Cassandra port to connect to (default 9042)
  -u CASSANDRA_USERNAME, --cassandra-username CASSANDRA_USERNAME
                        Cassandra username to ingest with (REQUIRED)
  -p CASSANDRA_PASSWORD, --cassandra-password CASSANDRA_PASSWORD
                        Cassandra password to authenticate with (REQUIRED)
  -pk PARTITION_KEY, --partition-key PARTITION_KEY
                        one or more partition keys, comma-separated, no spaces.
                        NOTE: this argument is required if Cassandra table is not yet created
                        examples:
                            partition_key
                            partition_key1,partition_key2
  -ck CLUSTER_KEY, --cluster-key CLUSTER_KEY
                        one or more cluster keys, comma-separated, no spaces
                        examples:
                           cluster_key
                           cluster_key1,cluster_key2
```

## TODO

- [X] Create Cassandra table if they do not exist
- [ ] Validate Avro data entries in FileStreamSource, drop if invalid
- [ ] Use deployed Kafka instead of locally running instance on localhost
- [ ] Allow user-specified Kafka server location
- [ ] SECURITY
