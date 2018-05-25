# delete_connector

Python script to delete a connector (source or sink)

## Usage:

```bash
usage: delete_connector [-h] [-d] [-g] [-k KAFKA_REST_URL]
                        [-kp KAFKA_REST_PORT] [-a]
                        [connectors [connectors ...]]

Deletes running Kafka connectors.

positional arguments:
  connectors            Avro schema file

optional arguments:
  -h, --help            show this help message and exit
  -d, --dry             dry run
  -g, --debug           debug this script
  -k KAFKA_REST_URL, --kafka-rest-url KAFKA_REST_URL
                        URL of kafka rest endpoint (default localhost)
  -kp KAFKA_REST_PORT, --kafka-rest-port KAFKA_REST_PORT
                        Port of kafka rest endpoint (default 8083)
  -a, --all             Delete all connectors
```
