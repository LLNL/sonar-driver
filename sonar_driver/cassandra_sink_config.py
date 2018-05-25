"""
Cassandra Sink Connector Configuration Class
"""

from connector_config import ConnectorConfig


class CassandraSinkConfig(ConnectorConfig):

    CONNECTOR_CLASS         = "com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector"
    
    CASSANDRA_KEYSPACE_KEY  = "connect.cassandra.key.space"
    CASSANDRA_HOST_KEY      = "connect.cassandra.contact.points"
    CASSANDRA_PORT_KEY      = "connect.cassandra.port"
    CASSANDRA_USERNAME_KEY  = "connect.cassandra.username"
    CASSANDRA_PASSFILE_KEY  = "connect.cassandra.password.file"
    KCQL_KEY                = "connect.cassandra.kcql"

    def __init__(self, 
            topic, 
            keyspace,
            table,
            cassandra_username,
            cassandra_password_file,
            cassandra_hosts=['localhost'],
            cassandra_port=9042,
            kcql=None, # will be inferred
            tasks_max=1):

        super().__init__(topic, tasks_max)

        self.config_dict[self.CASSANDRA_KEYSPACE_KEY]   = keyspace
        self.config_dict[self.CASSANDRA_HOST_KEY]       = ','.join(cassandra_hosts)
        self.config_dict[self.CASSANDRA_PORT_KEY]       = cassandra_port
        self.config_dict[self.CASSANDRA_USERNAME_KEY]   = cassandra_username
        self.config_dict[self.CASSANDRA_PASSFILE_KEY]   = cassandra_password_file

        if kcql is None:
            self.config_dict[self.KCQL_KEY] = "INSERT INTO " + table + " SELECT * FROM " + topic
        else:
            self.config_dict[self.KCQL_KEY] = kcql
