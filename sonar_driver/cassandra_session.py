from print_utils import pretty_print
from pygments import lexers

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraSession():
    def __init__(self, username, password_file, hosts=['localhost'], port=9042, dry=False, debug=False):
        self.dry = dry
        self.debug = debug

        if self.debug:
            print("Connecting to Cassandra hosts {} with username '{}' and password file '{}'".format(hosts, username, password_file))

        if not self.dry:
            with open(password_file, 'r') as pf:
                password=myfile.read()

            auth_provider = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster(hosts, port=port, auth_provider=auth_provider)

        if not self.dry:
            try:
                self.session = cluster.connect()
            except NoHostAvailableException:
                raise Exception("Cassandra host '{}' unavailable!".format(host))
            except UnauthorizedException:
                raise Exception("Cassandra user '{}' unauthorized to connect to host '{}'!".format(user,host))

    def table_exists(self, keyspace, table):

        exists_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'".format(keyspace, table)

        if self.debug:
            pretty_print("Check for Cassandra table CQL", exists_query, lexers.SqlLexer())

        if not self.dry:
            try:
                results = session.execute(exists_query)
            except UnauthorizedException:
                raise Exception("Cassandra user '{}' unauthorized to view system_schema.tables on host '{}'!".format(user,host))

            if self.debug:
                pretty_print("Query results", results.current_rows)

            if results.current_rows:
                return True
            else:
                return False
        else:
            return False # dry run, assume it's False to test

    @staticmethod
    def primary_key(partition_key, cluster_key):
        if cluster_key:
            return "(({}),{})".format(partition_key, cluster_key)
        return "(({}))".format(partition_key)

    def create_table_from_avro_schema(self, keyspace, table, avro_schema, partition_key, cluster_key):

        AVRO_CASSANDRA_TYPEMAP = {
            "string" : "text",
            "long" : "bigint"
        }

        avro2cass = lambda dtype: AVRO_CASSANDRA_TYPEMAP[dtype] if dtype in AVRO_CASSANDRA_TYPEMAP else dtype
        
        avro_json = avro_schema.to_json()
        columns_clause = ','.join(map(lambda f: f['name'] + ' ' + avro2cass(f['type']), avro_json['fields']))
        primary_key_clause = CassandraSession.primary_key(partition_key, cluster_key)

        create_query = "CREATE TABLE {}.{} ({}, PRIMARY KEY {})".format(keyspace, table, columns_clause, primary_key_clause)

        if self.debug or self.dry:
            pretty_print("Create table CQL", create_query, lexers.SqlLexer())
        if not self.dry:
            session.execute(create_query, timeout=None) 