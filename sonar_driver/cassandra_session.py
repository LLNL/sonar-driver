from pygments import lexers

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable
from cassandra import AuthenticationFailed
from sonar_driver.print_utils import pretty_print

class CassandraSession():
    def __init__(self, username, password_filename, hosts=['localhost'], port=9042, dry=False, debug=False):
        self.dry = dry
        self.debug = debug
        self.username = username
        self.hosts = hosts

        if self.debug:
            print("Connecting to Cassandra hosts {} with username '{}' and password file '{}'".format(self.hosts, self.username, password_filename))

        if not self.dry:
            with open(password_filename, 'r') as password_file:
                password=password_file.read().replace('\n', '')

            auth_provider = PlainTextAuthProvider(username=self.username, password=password)
            cluster = Cluster(self.hosts, port=port, auth_provider=auth_provider)

        if not self.dry:
            try:
                self.session = cluster.connect()
            except NoHostAvailable:
                raise Exception("Cassandra hosts '{}' unavailable!".format(self.hosts))
            except AuthenticationFailed:
                raise Exception("Cassandra user '{}' unauthorized to connect to hosts '{}'!".format(self.username,self.hosts))

    def table_exists(self, keyspace, table):

        exists_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'".format(keyspace, table)

        if self.debug:
            pretty_print(exists_query, title="Check for Cassandra table CQL", lexer=lexers.SqlLexer())

        if not self.dry:
            try:
                results = self.session.execute(exists_query)
            except AuthenticationFailed:
                raise Exception("Cassandra user '{}' unauthorized to view system_schema.tables on hosts '{}'!".format(self.username,self.hosts))

            if self.debug:
                pretty_print(results.current_rows, title="Query results")

            if results.current_rows:
                return True
            else:
                return False
        else:
            return True

    @staticmethod
    def avro2cass(avro_dtype):
        AVRO_CASSANDRA_TYPEMAP = {
            "string" : "text",
            "long" : "bigint"
        }

        if isinstance(avro_dtype, dict):
            if avro_dtype['type'] == 'array':
                return "list<" + CassandraSession.avro2cass(avro_dtype['values']) + ">"
            elif avro_dtype['type'] == 'map':
                return "map<text," + CassandraSession.avro2cass(avro_dtype['values']) + ">"

        return AVRO_CASSANDRA_TYPEMAP[avro_dtype] if avro_dtype in AVRO_CASSANDRA_TYPEMAP else avro_dtype

    @staticmethod
    def primary_key(partition_key, cluster_key):
        partition_key_parts = partition_key.split(',')
        partition_key_quoted = ','.join(map(lambda s: "\"" + s + "\"", partition_key_parts))

        if cluster_key:
            cluster_key_parts = cluster_key.split(',')
            cluster_key_quoted = ','.join(map(lambda s: "\"" + s + "\"", cluster_key_parts))
            return "(({}),{})".format(partition_key_quoted, cluster_key_quoted)

        return "(({}))".format(partition_key_quoted)

    def create_table_from_avro_schema(self, keyspace, table, avro_schema, partition_key, cluster_key):

        
        avro_json = avro_schema.to_json()
        columns_clause = ', '.join(map(lambda f: "\"" + f['name'] + "\"" + ' ' + CassandraSession.avro2cass(f['type']), avro_json['fields']))
        primary_key_clause = CassandraSession.primary_key(partition_key, cluster_key)

        create_query = "CREATE TABLE {}.{} ({}, PRIMARY KEY {})".format(keyspace, table, columns_clause, primary_key_clause)

        if self.debug or self.dry:
            pretty_print(create_query, title="Create table CQL", lexer=lexers.SqlLexer())
        if not self.dry:
            self.session.execute(create_query, timeout=None) 
