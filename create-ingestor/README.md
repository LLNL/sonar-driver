# create_ingestor

```bash
usage: create_ingestor [-h] [-d] [-i INGEST_FILE] [-p PRIMARY_KEY]
                       [-c CLUSTER_KEY]
                       avro_schema_file keyspace table

creates a Kafka ingestion file source for a Cassandra table, creating the table if it does not yet exist.

positional arguments:
  avro_schema_file      Avro schema file
  keyspace              Cassandra keyspace to ingest into
  table                 Cassandra table to ingest into

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           debug this script
  -i INGEST_FILE, --ingest-file INGEST_FILE
                        file to use as ingestion point, defaults to <table>.kafka
  -p PRIMARY_KEY, --primary_key PRIMARY_KEY
                        one or more primary keys, comma-separated, no spaces.
                        NOTE: this argument is required if Cassandra table is not yet created
                        examples:
                            primary_key
                            primary_key1,primary_key2
  -c CLUSTER_KEY, --cluster_key CLUSTER_KEY
                        one or more cluster keys, comma-separated, no spaces
                        examples:
                           cluster_key
                           cluster_key1,cluster_key2
```
