"""
Sonar Directory Source Connector Configuration Class
"""

from connector_config import ConnectorConfig
from enum import Enum


class FileFormat(Enum):
    JSON    = 'json'
    CSV     = 'csv'

    def __str__(self):
        return str(self.value)


class SonarDirectorySourceConfig(ConnectorConfig):

    CONNECTOR_CLASS         = "gov.llnl.sonar.kafka.connect.connectors.DirectorySourceConnector"
    BATCH_SIZE_KEY          = "batch.size" 
    DIRNAME_KEY             = "dirname" 
    COMPLETED_DIRNAME_KEY   = "completed.dirname" 
    FORMAT_KEY              = "format" 
    FORMAT_OPTIONS_KEY      = "format.options" 
    AVRO_SCHEMA_KEY         = "avro.schema" 

    def __init__(self, 
            topic, 
            dirname, 
            completed_dirname, 
            avro_schema,
            tasks_max=1, 
            file_format=FileFormat.JSON, 
            format_options={}, 
            batch_size=10000):

        super().__init__(topic, tasks_max)

        self.config_dict[self.BATCH_SIZE_KEY]           = batch_size
        self.config_dict[self.DIRNAME_KEY]              = dirname
        self.config_dict[self.COMPLETED_DIRNAME_KEY]    = completed_dirname
        self.config_dict[self.FORMAT_KEY]               = file_format
        self.config_dict[self.FORMAT_OPTIONS_KEY]       = format_options
        self.config_dict[self.AVRO_SCHEMA_KEY]          = avro_schema

