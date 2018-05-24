#!/usr/bin/env bash

pdsh -w rzsonar[8-10] 'cp ~/drop/sonar-connectors-0.3.jar /tmp/kafkatest && chmod a+rx /tmp/kafkatest/sonar-connectors-0.3.jar'
