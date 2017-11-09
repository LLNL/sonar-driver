import sh
import argparse
import avro


def parse_args():
    """ Parse arguments """

    parser = argparse.ArgumentParser(
            description="Creates a Kafka ingestion file source for a Cassandra table,"
                        "creating the table if it does not yet exist.",
            formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-i', '--ingest-file', required=False,
            help="File to use as ingestion point")
    parser.add_argument('-a', '--avro-schema-file', required=True,
            help="REQUIRED Avro schema file")
    parser.add_argument('-k', '--keyspace', type=str, required=True,
            help="REQUIRED Keyspace in which to create the table")
    parser.add_argument('-t', '--table_name', type=str, required=True,
            help="REQUIRED Name for the table to create")
    parser.add_argument('-p', '--primary_key', type=str, required=False,
            help="One or more primary keys, comma-separated, no spaces."
                "\nNOTE: This argument is required if Cassandra table is not yet created!"
                "\nExamples:"
                "\n    primary_key"
                "\n    primary_key1,primary_key2")
    parser.add_argument('-c', '--cluster_key', type=str, required=False,
            help="One or more cluster keys, comma-separated, no spaces"
                 "\nExamples:"
                 "\n   cluster_key"
                 "\n   cluster_key1,cluster_key2")

    return parser.parse_args()


def main():
    """ Main entrypoint """

    print(parse_args())

if __name__ == '__main__':
    main()
