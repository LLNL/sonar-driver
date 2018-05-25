from sonar_driver.print_utils import pretty_print
from sonar_driver.kafka_connect_session import KafkaConnectSession

def delete_connectors(
        connectors,
        kafka_rest_url='localhost',
        kafka_rest_port=8083,
        debug=False,
        dry=False):

    # Initialize Kafka Connect REST session
    kafka_connect_session = KafkaConnectSession(
        kafka_rest_url,
        kafka_rest_port,
        debug, 
        dry
    )

    # Issue DELETE for each
    for connector in connectors:
        kafka_connect_session.request('DELETE', '/connectors/' + connector, 204)

    # Get remaining connectors
    connectors_response = kafka_connect_session.request('GET', '/connectors', 200)
    if not dry:
        connectors = connectors_response.json()
    else:
        connectors = []
    pretty_print("Connectors remaining", connectors)


def delete_all_connectors(
        kafka_rest_url='localhost',
        kafka_rest_port=8083,
        debug=False,
        dry=False):

    # Initialize Kafka Connect REST session
    kafka_connect_session = KafkaConnectSession(
        kafka_rest_url,
        kafka_rest_port,
        debug, 
        dry
    )

    # Get connectors to delete
    connectors_response = kafka_connect_session.request('GET', '/connectors', 200)
    if not dry:
        connectors = connectors_response.json()
    else:
        connectors = ['all', 'of', 'them']

    delete_connectors(connectors, kafka_rest_url, kafka_rest_port, debug, dry)
    

