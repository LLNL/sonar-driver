# Sonar Driver

This library contains the logic to drive Sonar, including:

* Creating Kafka Connect sources and sinks:
    * Sonar directory source
    * Cassandra sink
* Communicating with the Kafka REST API
* Communicating with Cassandra

## Quick Start Guide

See [quickstart.md](./doc/quickstart.md)

## Prerequisites

* Python3

* Python dependencies listed in [requirements.txt](./requirements.txt)

## Usage

```bash

usage: create-ingestor [-h] [-d] [-g] [-f FILE_FORMAT] [-fo FORMAT_OPTIONS]
                       [-t MAX_TASKS] [-b BATCH_SIZE] -i INGEST_DIR -o
                       COMPLETED_DIR [-k KAFKA_REST_URL] [-kp KAFKA_REST_PORT]
                       [-c CASSANDRA_HOST] [-cp CASSANDRA_PORT] -u
                       CASSANDRA_USERNAME -p CASSANDRA_PASSWORD_FILE
                       [-pk PARTITION_KEY] [-ck CLUSTER_KEY]
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
  -f FILE_FORMAT, --file-format FILE_FORMAT
                        file format (json|csv)
  -fo FORMAT_OPTIONS, --format-options FORMAT_OPTIONS
                        file format options
  -t MAX_TASKS, --max-tasks MAX_TASKS
                        maximum number of concurrent ingestion tasks
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        batch size for file reads
  -i INGEST_DIR, --ingest-dir INGEST_DIR
                        directory to use as ingestion point
  -o COMPLETED_DIR, --completed-dir COMPLETED_DIR
                        directory to move ingested files into
  -k KAFKA_REST_URL, --kafka-rest-url KAFKA_REST_URL
                        URL of kafka rest endpoint (default localhost)
  -kp KAFKA_REST_PORT, --kafka-rest-port KAFKA_REST_PORT
                        Port of kafka rest endpoint (default 8083)
  -c CASSANDRA_HOST, --cassandra-host CASSANDRA_HOST
                        Cassandra host to connect to (default localhost)
  -cp CASSANDRA_PORT, --cassandra-port CASSANDRA_PORT
                        Cassandra port to connect to (default 9042)
  -u CASSANDRA_USERNAME, --cassandra-username CASSANDRA_USERNAME
                        Cassandra username to ingest with (REQUIRED)
  -p CASSANDRA_PASSWORD_FILE, --cassandra-password-file CASSANDRA_PASSWORD_FILE
                        Cassandra password file to authenticate with (REQUIRED)
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
