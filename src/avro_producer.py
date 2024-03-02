import logging
import os
from uuid import uuid4

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

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


def delivery_report(err, msg):
    if err is not None:
        logging.error(
            f"Delivery failed for User record for {msg.key()} with error {err}"
        )
        return
    logging.info(
        f"Successfully produced User record: key - {msg.key()}, topic - {msg.topic}, partition - {msg.partition()}, offset - {msg.offset()}"
    )


class AvroProducer(ProducerClass):
    def __init__(self, bootstrap_server, topic, schema_registry_client, schema_str):
        super().__init__(bootstrap_server, topic)
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        self.string_serializer = StringSerializer("utf-8")

    def send_message(self, message):
        try:
            message = self.avro_serializer(
                message, SerializationContext(topic, MessageField.VALUE)
            )
            self.producer.produce(
                topic=self.topic,
                key=self.string_serializer(str(uuid4())),
                value=message,
                headers={"correlation_id": str(uuid4())},
                on_delivery=delivery_report,
            )
            logging.info(f"Message sent successfully: {message}")
        except Exception as e:
            logging.error(f"Error while sending message: {e}")


if __name__ == "__main__":
    utils.load_env()
    logging_config.configure_logging()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC")
    schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    schema_type = "AVRO"

    # Create Topic
    admin = Admin(bootstrap_servers)
    admin.create_topic(topic)

    # Register the Schema
    with open("./schemas/schema.avsc") as avro_schema_file:
        avro_schema = avro_schema_file.read()
    schema_client = SchemaClient(schema_registry_url, topic, avro_schema, schema_type)
    schema_client.register_schema()

    # fetch schema_str from Schema Registry
    schema_str = schema_client.get_schema_str()
    # Produce messages
    producer = AvroProducer(
        bootstrap_servers, topic, schema_client.schema_registry_client, schema_str
    )

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
            # Prior to serialization, all values must first be converted to a dict instance.
            producer.send_message(user_to_dict(user))
    except KeyboardInterrupt:
        pass

    producer.commit()
