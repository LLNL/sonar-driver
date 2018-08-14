# Sonar Driver

This library contains the logic to drive Sonar, including:

### Analysis
* [Connecting to Sonar's Cassandra Database](./doc/cassandra.md)
* [Running Spark on Sonar](./doc/spark.md)

### Ingestion
* [Creating Kafka Connect sources and sinks](./doc/creating_connectors.md)
* [Installing and uninstalling Kafka Connect sources and sinks](./doc/installing_connectors.md)
* [Creating Cassandra tables from Avro schema files](./doc/creating_cassandra_tables.md)
* Communicating with the Kafka REST API

## Prerequisites

* python3
* make

Python library dependencies will be automatically collected from [requirements.txt](./requirements.txt)

## Install

Assuming you cloned this repo into location SONAR_DRIVER_HOME

1-step install: `source install.sourceme`

Or, build components separately using:

1. Run `make` to create the necessary python virtualenv for this project.

2. Invoke the virtual environment with `source ${SONAR_DRIVER_HOME}/venv/bin/activate`

3. Run `make sonar-driver` to install the project into the current `PYTHONPATH`.

4. Add `${SONAR_DRIVER_HOME}/bin` to `PATH` to run commands anywhere

# Running

The following environment variables may be used by the commands in `bin` if set:

```bash
KAFKA_REST_URL      # e.g. http://sonar8
KAFKA_REST_PORT     # e.g. 8083
CQLSH_HOST          # e.g. sonar8
CQLSH_PORT          # e.g. 9042
ZOOKEEPER_HOST      # e.g. sonar8
ZOOKEEPER_PORT      # e.g. 2181
```
