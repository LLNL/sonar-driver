from pygments import lexers

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable
from cassandra import AuthenticationFailed

import getpass

from sonar_driver.print_utils import pretty_print, eprint


class CassandraSession():
    def __init__(self, password_filename=None, password_token=None, hosts=['localhost'], port=9042, dry=False, debug=False):

        self.dry = dry
        self.debug = debug
        self.username = getpass.getuser()
        self.hosts = hosts
        self.hosts_string = ','.join(hosts)

        password_args = sum(1 for p in [password_filename, password_token] if p is not None)
        if (password_args > 1):
            raise Exception("Multiple password arguments were supplied! Failing due to ambiguity.")

        if not self.dry:

            # Use a password arg
            if (password_args == 1):
                if password_filename is not None:
                    with open(password_filename, 'r') as password_file:
                        password = password_file.read().replace('\n', '')
                elif password_token is not None:
                    self.token = password_token
                    password = password_token

                auth_provider = PlainTextAuthProvider(username=self.username, password=password)
                cluster = Cluster(self.hosts, port=port, auth_provider=auth_provider)

                self.session = cluster.connect()

            # Interactively authenticate
            else:
                self.init_interactive(hosts)

    def get_crowd_token(self):
        query = "SELECT value FROM {}_k.tokencache WHERE attribute='encrypted token'".format(self.username)
        results = self.session.execute(query, timeout=None)
        return results[0].value

    def init_interactive(self, token=None):

        for x in range(2): # 2 retries

            if token is None:
                token = CassandraSession.get_crowd_token_interactive(self.hosts)

            auth_provider = PlainTextAuthProvider(username=self.username, password=token) 
            cluster = Cluster(self.hosts, auth_provider=auth_provider)

            try:
                self.session = cluster.connect()
                self.token = token # success!
            except NoHostAvailable as e:
                if isinstance(list(e.errors.values())[0], AuthenticationFailed):
                    eprint("Token authentication failure")
                    token = None
                    continue

    def get_crowd_token_interactive(self):

        for x in range(3): # 3 retries

            password = getpass.getpass("Enter LC Pin+OTP for user {}:".format(self.username))
            clear_output()

            auth_provider = PlainTextAuthProvider(username=self.username, password=password) 
            cluster = Cluster(self.hosts, auth_provider=auth_provider)

            try:
                self.session = cluster.connect()
                self.token = self.get_crowd_token()
                self.session.shutdown()

                return self.token

            except NoHostAvailable as e:
                if isinstance(list(e.errors.values())[0], AuthenticationFailed):
                    eprint("Pin+OTP Authentication Failure")
                    continue

            except IndexError:
                eprint("Encrypted token not found! Contact Cassandra system administrator.")

        raise AuthenticationFailed("Could not get CROWD token for user {}".format(self.username))

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
