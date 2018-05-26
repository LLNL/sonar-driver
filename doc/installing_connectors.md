# Installing and Uninstalling Connectors

Connectors are JSON objects that can be created using the connector creator scripts (see [Creating Connectors](./creating_connectors.md)).
Created connectors can then be installed using the `install_connectors` script or uninstalled using the `uninstall_connectors` script.
Invoke with `--dry/-d` or `--debug/-g` to see the ensuing REST commands.

## Connecting to Kafka

Specify the Kafka REST endpoint either in the command line:

```bash
install_connector --kafka-rest-url http://sonar8 --kafka-rest-port 8083 ...
```

...or using environment variables:

```bash
KAFKA_REST_URL=http://sonar8 KAFKA_REST_PORT=8083 install_connector ...
```

## Installing Connectors

If you have created connectors `my_source.json` and `my_sink.json`, simply:

```bash
install_connector my_source.json my_sink.json
```

You can also pipe the output of a connector creator script into `install_connectors -`, which reads from stdin.

```bash
create_cassandra_sink_connector -u theuser -p passfile mykey mytable | install_connectors -
```

## Uninstalling Connectors

Provide the name(s) of the connector to uninstall as specified in the "name" field of the connector JSON object.

```bash
uninstall_connectors my_source_name my_sink_name
```
