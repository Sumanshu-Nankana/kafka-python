"""The Producer code will Serialize the JSON message.

by getting the schema from the Schema Registry
CANNOT test - https://github.com/redpanda-data/redpanda/issues/14462
Redpanda does not support JSON Schema Registry
"""
import json
import logging
import os

import logging_config
import utils
from admin import Admin
from producer import ProducerClass
from schema_registry_client import SchemaClient


class User:
    def __init__(self, first_name, middle_name, last_name, age):
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.age = age


def user_to_dict(user):
    """Return a dictionary representation of a User instance  for
    serialization."""
    return dict(
        first_name=user.first_name,
        middle_name=user.middle_name,
        last_name=user.last_name,
        age=user.age,
    )


class JSONProducer(ProducerClass):
    def __init__(self, bootstrap_server, topic):
        super().__init__(bootstrap_server, topic)
        self.value_serializer = lambda v: json.dumps(v).encode("utf-8")

    def send_message(self, message_dict):
        try:
            # Convert message to JSON string and serialize
            message_json = self.value_serializer(message_dict)
            self.producer.produce(self.topic, message_json)
            logging.info(f"Message Sent: {message_json}")
        except Exception as e:
            logging.error(f"Error sending message: {e}")


if __name__ == "__main__":
    utils.load_env()
    logging_config.configure_logging()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC")
    schema_url = os.environ.get("SCHEMA_URL")

    """
    Reading using read() function, because SchemaClient register_schema
    function required schema in form of string
    """
    with open("./schemas/schema.json") as json_schema_file:
        json_schema = json_schema_file.read()

    admin = Admin(bootstrap_servers)
    admin.create_topic(topic)

    # Can not test in REDPANDA, Because REDPANDA does not support JSON
    # https://github.com/redpanda-data/redpanda/issues/14462
    schema_client = SchemaClient(schema_url, topic, json_schema, "JSON")
    schema_client.register_schema()

    producer = JSONProducer(bootstrap_servers, topic)

    try:
        while True:
            first_name = input("Enter first name: ")
            middle_name = input("Enter middle name: ")
            last_name = input("Enter last name: ")
            age = int(input("Enter age: "))
            user = User(
                first_name=first_name,
                middle_name=middle_name,
                last_name=last_name,
                age=age,
            )
            producer.send_message(user_to_dict(user))
    except KeyboardInterrupt:
        pass

    producer.commit()
