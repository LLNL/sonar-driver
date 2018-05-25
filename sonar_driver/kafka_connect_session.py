import requests

from print_utils import pretty_print


class KafkaConnectSession():

    def __init__(self, kafka_rest_url='localhost', kafka_rest_port=8083, debug=False, dry=False):
        self.requests_session = requests.Session()
        self.debug = debug
        self.dry = dry
        self.kafka_rest_url = kafka_rest_url
        self.kafka_rest_port = kafka_rest_port
        self.kafka_rest_endpoint = self.kafka_rest_url + ":" + str(self.kafka_rest_port)

    def request(self, command, suburl, json=None, expected_status_code=201):
        request = requests.Request(
            command, 
            self.kafka_rest_endpoint + suburl, 
            json=json)
        prepared_request = request.prepare()

        if self.dry or self.debug:
            pretty_print("Connector HTTP Request", request.__dict__)
        if not self.dry:
            response = self.requests_session.send(prepared_request)
            if self.debug:
                pretty_print("Connector HTTP Response", response.json())
            if (response.status_code != expected_status_code):
                raise Exception("Error: status code {} != expected status code {}! Run with -g/--debug to see server response".format(response.status_code, expected_status_code))
            return response

    def create_connector(self, connector):
        self.request('POST', '/connectors/', connector.json())
