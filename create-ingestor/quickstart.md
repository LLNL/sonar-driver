# Quick Start Guide

## Install Python Dependencies

Using pip:

```bash
pip3 install -r requirements.txt
```

## Local Setup of Confluent (Kafka), Cassandra, and CassandraSink

Make sure `JAVA_HOME` is set to a Java 8 installation (e.g., by executing `module load java/1.8.0`)

Download Confluent and Cassandra:

```bash
(localhost):~$ mkdir local_setup && cd local_setup
(localhost):local_setup$ curl -L http://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz | tar xz
(localhost):local_setup$ curl -L http://mirror.reverse.net/pub/apache/cassandra/3.11.1/apache-cassandra-3.11.1-bin.tar.gz | tar xz
```

Download the CassandraSink plugin and make it available to Confluent by placing it in a new directory `confluent-4.0.0/share/java/kafka-connect-cassandra`:

```bash
(localhost):local_setup$ curl -L https://github.com/Landoop/stream-reactor/releases/download/0.3.0/kafka-connect-cassandra-0.3.0-3.3.0-all.tar.gz | tar xz
(localhost):local_setup$ mkdir confluent-4.0.0/share/java/kafka-connect-cassandra
(localhost):local_setup$ mv kafka-connect-cassandra-0.3.0-3.3.0-all.jar confluent-4.0.0/share/java/kafka-connect-cassandra
```

Start up Confluent connect:

```bash
(localhost):local_setup$ ./confluent-4.0.0/bin/confluent start connect
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
```

Check to make sure the CassandraSink plugin is loaded:

```bash
(localhost):local_setup$ ./confluent-4.0.0/bin/confluent status plugins | grep CassandraSinkConnector
    "class": "com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector",
```

Start up Cassandra:

```bash
(localhost):local_setup$ ./apache-cassandra-3.11.1/bin/cassandra
```

## Example Usage

1. Create an ingestion point for an existing Cassandra table

```bash
(localhost):sonar-user-tools$ ./create-ingestor idstr.avsc test idstr -u user -p password
(localhost):sonar-user-tools$ ls
README.md  create-ingestor*  idstr.avsc  test.idstr.kafka
(localhost):sonar-user-tools$ echo '{"id":80, "str": "eighty"}' >> test.idstr.kafka
(localhost):sonar-user-tools$ cqlsh --cqlversion="3.4.0" -e "SELECT * FROM test.idstr"

 id | str
----+--------
 80 | eighty
```

2. Create an ingestion point for a non-existing Cassandra table, thus creating the table (requires primary key definition):

```bash
(localhost):sonar-user-tools$ ./create-ingestor idstr.avsc test idstr --primary-key id -u user -p password
(localhost):sonar-user-tools$ ls
README.md  create-ingestor*  idstr.avsc  test.idstr.kafka  test.idstr2.kafka
(localhost):sonar-user-tools$ echo '{"id":80, "str": "eighty"}' >> test.idstr2.kafka
(localhost):sonar-user-tools$ cqlsh --cqlversion="3.4.0" -e "SELECT * FROM test.idstr2"

 id | str
----+--------
 80 | eighty
```

View running connectors in Confluent:

```bash
(localhost):create-ingestor$ confluent status connectors
[
  "avro-file-source-test.idstr",
  "avro-cassandra-sink-test.idstr",
  "avro-file-source-test.idstr2",
  "avro-cassandra-sink-test.idstr2"
]
```

