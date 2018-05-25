#!/usr/bin/env python

import sys
import os
import time
import argparse
import traceback

import avro.schema

from sonar_driver.kafka_connect_session import KafkaConnectSession
from sonar_driver.cassandra_session import CassandraSession

from sonar_driver.print_utils import pretty_print
from sonar_driver.connector import Connector
from sonar_driver.sonar_directory_source_config import SonarDirectorySourceConfig, FileFormat
from sonar_driver.cassandra_sink_config import CassandraSinkConfig


def create_ingestor_connectors(
        avro_schema, 
        keyspace,
        table,
        ingest_dir,
        completed_dir,
        cassandra_username,
        cassandra_password_file,
        file_format=FileFormat('json'),
        format_options={},
        batch_size=10000,
        cassandra_hosts=['localhost'],
        cassandra_port=9042,
        tasks_max=1,
        debug=False,
        dry=False):

    """ Create directory source and Cassandra sink connector objects """

    # Get absolute paths
    ingest_dir_abspath = os.path.abspath(ingest_dir)
    completed_dir_abspath = os.path.abspath(completed_dir)

    # Create unique topic
    topic_name = str(int(time.time())) + "-" + str(hash(ingest_dir_abspath)) + "--" + keyspace + "." + table

    # Create Connector objects
    directory_source = Connector(
        "sonar-directory-source-" + topic_name,
        SonarDirectorySourceConfig(
            topic_name,
            ingest_dir_abspath,
            completed_dir_abspath,
            avro_schema,
            tasks_max=tasks_max,
            file_format=file_format,
            format_options=format_options,
            batch_size=batch_size
        )
    )
    cassandra_sink = Connector(
        "cassandra-sink-" + topic_name,
        CassandraSinkConfig(
            topic_name,
            keyspace,
            table,
            cassandra_username,
            cassandra_password_file,
            cassandra_hosts=cassandra_hosts,
            cassandra_port=cassandra_port,
            tasks_max=tasks_max
        )
    )

    if debug:
        pretty_print("Sonar directory connector", directory_source.json())
        pretty_print("Cassandra sink connector", cassandra_sink.json())

    return [directory_source, cassandra_sink]


def create_ingestor(
        avro_schema,
        keyspace,
        table,
        ingest_dir,
        completed_dir,
        cassandra_username,
        cassandra_password_file,
        file_format='json',
        format_options={},
        batch_size=10000,
        kafka_rest_url='localhost', 
        kafka_rest_port=8083, 
        cassandra_hosts=['localhost'],
        cassandra_port=9042,
        partition_key=None,
        cluster_key=None,
        tasks_max=1,
        debug=False,
        dry=False):

    # Initialize Kafka Connect REST session
    kafka_connect_session = KafkaConnectSession(
        kafka_rest_url,
        kafka_rest_port,
        debug, 
        dry
    )

    # Check for directory source and cassandra sink connector plugins
    required_classes = [
        SonarDirectorySourceConfig.CONNECTOR_CLASS, 
        CassandraSinkConfig.CONNECTOR_CLASS
    ]
    if not dry:
        plugins = kafka_connect_session.request('GET', '/connector-plugins').json()
        plugin_classes = [plugin['class'] for plugin in plugins]
        for required_class in required_classes:
            if required_class not in plugin_classes:
                raise Exception("Connector class not available: '{}'".format(required_class))

    # Check if Cassandra table exists, and if not, try to make it
    cassandra_session = CassandraSession(
        cassandra_username, 
        cassandra_password_file, 
        hosts=cassandra_hosts,
        port=cassandra_port, 
        dry=dry, 
        debug=debug
    )
    if not cassandra_session.table_exists(keyspace, table):
        if partition_key is not None:
            cassandra_session.create_table_from_avro_schema(
                keyspace,
                table,
                avro_schema,
                partition_key,
                cluster_key
            )
        else:
            raise Exception("Table {}.{} does not exist, and no partition key was defined to create it!".format(keyspace, table))

    # Create the connector objects
    connectors = create_ingestor_connectors(
        avro_schema,
        keyspace,
        table,
        ingest_dir,
        completed_dir,
        cassandra_username,
        cassandra_password_file,
        file_format,
        format_options,
        batch_size,
        cassandra_hosts,
        cassandra_port,
        tasks_max,
        debug,
        dry)

    # Create the connectors!
    for connector in connectors:
        kafka_connect_session.create_connector(connector)
