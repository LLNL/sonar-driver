#!/usr/bin/env bash

# make self-aware
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# activate python virtual environment
source $DIR/virtualenv/bin/activate

# remove existing connectors
$DIR/delete-all-ingestors -k http://rzsonar8

# create test data, readable by flume
# pdsh -w rzsonar[8-10] cp -r $DIR/testdir/ /usr/global/tools/lcmon/tmp/
# chmod -R a+rwx /usr/global/tools/lcmon/tmp/testdir
# mkdir /usr/global/tools/lcmon/tmp/testdir_out
# chmod -R a+rwx /usr/global/tools/lcmon/tmp/testdir_out

# create the ingestor
$DIR/create-ingestor -g -c rzsonar8 -t 9 -i /usr/global/tools/lcmon/tmp/test_smallfiles -o /usr/global/tools/lcmon/tmp/test_smallfiles_out -u kafka:flume -p /data/nvme0/cassandra-secrets/kafka $DIR/idstr.avsc flume_k test2 -k http://rzsonar8
