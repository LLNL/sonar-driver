"""
Kafka Connector Configuration Class
"""

from print_utils import pretty_print
import json


class ConnectorConfig():

    CONNECTOR_CLASS = "UNDEFINED"
    CONNECTOR_CLASS_KEY = "connector.class"
    TOPIC_KEY = "topic"
    TASKS_MAX_KEY = "tasks.max"

    def __init__(self, topic, tasks_max=1):
        self.config_dict = {}
        self.config_dict[self.CONNECTOR_CLASS_KEY] = self.CONNECTOR_CLASS
        self.config_dict[self.TOPIC_KEY] = topic
        self.config_dict[self.TASKS_MAX_KEY] = tasks_max

    def __str__(self):
        return str(self.json())

    def json(self):
        return {k: str(v) for k,v in self.config_dict.items()}
