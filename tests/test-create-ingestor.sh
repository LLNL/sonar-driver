#!/usr/bin/env bash

# make self-aware
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

${DIR}/../sonar_driver/create_ingestor \
    --debug \
    --dry \
    --cassandra-host cassandra_host \
    --kafka-rest-url kafka_rest_url \
    --cassandra-username cassandra_user \
    --cassandra-password-file cassandra_password_file \
    --file-format json \
    --format-options "{\"option\":\"myoption\"}" \
    --tasks-max 1 \
    --batch-size 40000 \
    --ingest-dir "mydirname" \
    --completed-dir "mycompleteddirname" \
    --partition-key "id" \
    ${DIR}/idstr.avsc mykeyspace mytable

