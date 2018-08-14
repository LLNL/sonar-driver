#!/usr/bin/env bash

CLUSTER=${HOSTNAME//[0-9]/}

if [[ "${CLUSTER}" == rzsonar ]]
then
    export KAFKA_REST_URL=http://rzsonar8
    export KAFKA_REST_PORT=8083
    export ZOOKEEPER_HOST=rzsonar8
    export ZOOKEEPER_PORT=2181
    export CQLSH_HOST=rzsonar8
    export CQLSH_PORT=9042
elif [[ "${CLUSTER}" == sonar ]]
then
    export KAFKA_REST_URL=http://sonar11
    export KAFKA_REST_PORT=8083
    export ZOOKEEPER_HOST=sonar11
    export ZOOKEEPER_PORT=2181
    export CQLSH_HOST=sonar11
    export CQLSH_PORT=9042
fi
