# create-ingestor

## Usage

```bash
usage: create-ingestor [-h] [-d] [-i INGEST_FILE] [-p PRIMARY_KEY]
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

## Example Usage

```bash
(sonar11):create-ingestor$ confluent start connect
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
(sonar11):sonar-user-tools$ ./create-ingestor idstr.avsc test idstr
(sonar11):sonar-user-tools$ ls
README.md  create-ingestor*  idstr.avsc  test.idstr.kafka
(sonar11):sonar-user-tools$ echo '{"id":80, "str": "eighty"}' >> test.idstr.kafka
(sonar11):sonar-user-tools$ cqlsh --cqlversion="3.4.0" -u cassandra -p cassandra -e "SELECT * FROM test.idstr"

 id | str
----+--------
 80 | eighty
```

## TODO

- [ ] Create Cassandra table if they do not exist
- [ ] Validate Avro data entries in FileStreamSource, drop if invalid
- [ ] Use deployed Kafka instead of locally running instance on localhost
- [ ] Allow user-specified Kafka server location
- [ ] SECURITY
