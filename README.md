# Sonar Driver

This library contains the logic to drive Sonar, including:

* [Creating Kafka Connect sources and sinks](./doc/creating_connectors.md)
* [Installing and uninstalling Kafka Connect sources and sinks](./doc/installing_connectors.md)
* [Creating Cassandra tables from Avro schema files](./doc/creating_cassandra_tables.md)
* Communicating with the Kafka REST API

## Prerequisites

* Python3
* virtualenv
* make

Python library dependencies will be automatically collected from [requirements.txt](./requirements.txt)

## Install

1. Run `make` to create the necessary python virtualenv for this project.

2. Invoke the virtualenv with `source ./virtualenv/bin/activate`

3. Run `make install` to install the project into the current `PYTHONPATH`.

4. Add the full path of `./bin` to `PATH` to run commands anywhere

