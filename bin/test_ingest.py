#!/usr/bin/env python

import os
import sh
import sys
import traceback
import avro.schema
import time
import argparse

from sonar_driver.kafka_connect.connector import Connector
from sonar_driver.kafka_connect.sonar_directory_source_config import SonarDirectorySourceConfig
from sonar_driver.kafka_connect.cassandra_sink_config import CassandraSinkConfig
from sonar_driver.kafka_connect.session import KafkaConnectSession


def parse_args():
    """ Parse arguments """

    parser = argparse.ArgumentParser(
        description="Installs Kafka connectors to a running Kafka Connect instance",
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-d', '--dry', action="store_true", required=False,
                        help="Dry run")

    # Test parameters
    parser.add_argument('-i', '--ingest-dir', default='/usr/global/tools/lcmon/tmp/testdir', required=False,
                        help="Directory source to ingest from")
    parser.add_argument('-o', '--completed-dir', default='/usr/global/tools/lcmon/tmp/testdir_out', required=False,
                        help="Directory to write ingested files into")
    parser.add_argument('-N', '--num-files', default=100, required=False,
                        help="Number of files to ingest")
    parser.add_argument('-n', '--rows-per-file', default=1000, required=False,
                        help="Number of rows in each file to ingest")
    parser.add_argument('-dT', '--directory-source-tasks', default="1", required=False,
                        help="Maximum number of concurrent directory source tasks")

    # Kafka
    parser.add_argument('-k', '--kafka-rest-url', default=os.environ.get("KAFKA_REST_URL", "localhost"), required=False,
                        help="URL of kafka rest endpoint (default: value of KAFKA_REST_URL or 'localhost')")
    parser.add_argument('-kp', '--kafka-rest-port', default=os.environ.get("KAFKA_REST_PORT", "8083"), required=False,
                        help="Port of kafka rest endpoint (default: value of KAFKA_REST_PORT or 8083)")

    # Cassandra
    parser.add_argument('-ck', '--cassandra-keyspace', default="test", required=False,
                        help="Cassandra keyspace to ingest into")
    parser.add_argument('-ct', '--cassandra-table', default="idstr", required=False,
                        help="Cassandra table to ingest into")
    parser.add_argument('-cT', '--cassandra-sink-tasks', default="1", required=False,
                        help="Maximum number of concurrent Cassandra sink tasks")
    parser.add_argument('-c', '--cassandra-hosts', nargs='+', default=[os.environ.get("CQLSH_HOST", "localhost")], required=False,
                        help="Cassandra host to connect to (default: value of CQLSH_HOST or 'localhost')")
    parser.add_argument('-cp', '--cassandra-port', default=os.environ.get("CQLSH_PORT", "9042"), required=False,
                        help="Cassandra port to connect to (default: value of CQLSH_PORT or 9042)")
    parser.add_argument('-u', '--cassandra-username', required=True,
                        help="Cassandra username to ingest with (REQUIRED)")
    parser.add_argument('-p', '--cassandra-password-file', required=True,
                        help="Cassandra password file to authenticate with (REQUIRED)")
    parser.add_argument('-br', '--batch-rows', default="10000", required=False,
                        help="number of rows to batch read per file")
    parser.add_argument('-bf', '--batch-files', default="10", required=False,
                        help="number of files to batch read")
    parser.add_argument('-zh', '--zookeeper-host', default=os.environ.get("ZOOKEEPER_HOST", "localhost"), required=False,
                        help="ZooKeeper host to connect to (default: value of ZOOKEEPER_HOST or 'localhost')")
    parser.add_argument('-zp', '--zookeeper-port', default=os.environ.get("ZOOKEEPER_PORT", "2181"), required=False,
                        help="ZooKeeper port to connect to (default: value of ZOOKEEPER_PORT or 2181)")
    parser.add_argument('-ds', '--delete-sunk-kafka-records', action="store_true",
                        help="Whether to delete Kafka records after they are sunk into Cassandra")

    args = parser.parse_args()

    return args


def main():
    """ Main entrypoint """

    args = parse_args()

    try:

        topic_name = 'test-' + str(time.time())

        avro_schema = avro.schema.Parse("""
            {
                "type": "record",
                "name": "idstr",
                "fields": [ 
                    { 
                        "name": "id",
                        "type": "int" 
                    },
                    { 
                        "name": "str",
                        "type": "string" 
                    } 
                ]
            }
            """)

        if not args.dry:

            # Create clean input/output directories
            sh.rm('-rf', args.ingest_dir)
            sh.rm('-rf', args.completed_dir)
            sh.mkdir('-p', args.ingest_dir)
            sh.mkdir('-p', args.completed_dir)

            # Write test data
            counter = 1
            for f in range(args.num_files):
                with open(os.path.join(args.completed_dir, "file_{:04d}".format(f)), "w") as file:
                    for n in range(args.rows_per_file):
                        file.write('{{ "id": {}, "str": "{}" }}\n'.format(counter, counter))
                        counter = counter + 1

        # Create directory source
        directory_source = Connector(
            "sonar_directory_source-" + topic_name,
            SonarDirectorySourceConfig(
                topic_name,
                args.ingest_dir,
                args.completed_dir,
                avro_schema,
                tasks_max=args.directory_source_tasks,
                file_format='json',
                batch_rows=args.batch_rows,
                batch_files=args.batch_files,
                zookeeper_host=args.zookeeper_host,
                zookeeper_port=args.zookeeper_port
            )
        )

        # Create Cassandra sink
        cassandra_sink = Connector(
            "cassandra_sink-" + topic_name,
            CassandraSinkConfig(
                topic_name,
                args.cassandra_keyspace,
                args.cassandra_table,
                args.cassandra_username,
                args.cassandra_password_file,
                cassandra_hosts=args.cassandra_hosts,
                cassandra_port=args.cassandra_port,
                kcql=None,
                tasks_max=args.cassandra_sink_tasks,
                delete_sunk_kafka_records=args.delete_sunk_kafka_records
            )
        )

        # Create a Kafka connect session to install connectors
        kafka_connect_session = KafkaConnectSession(
            args.kafka_rest_url,
            args.kafka_rest_port,
            True,
            args.dry
        )

        # Install connectors
        kafka_connect_session.install_connector(directory_source.json())
        kafka_connect_session.install_connector(cassandra_sink.json())

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stderr)
        return 1


if __name__ == '__main__':
    main()
