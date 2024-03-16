import logging
import os

from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import logging_config
import utils
from schema_registry_client import SchemaClient


class AvroConsumerClass:
    def __init__(
        self, bootstrap_server, topic, group_id, schema_registry_client, schema_str
    ):
        """Initializes the consumer."""
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_server,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
            }
        )
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    def consume_messages(self):
        """Consume Messages from Kafka."""
        self.consumer.subscribe([self.topic])
        logging.info(f"Successfully subscribed to topic: {self.topic}")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
                byte_message = msg.value()
                decoded_message = self.avro_deserializer(
                    byte_message, SerializationContext(topic, MessageField.VALUE)
                )
                logging.info(
                    f"Decoded message: {decoded_message}, Type: {type(decoded_message)}"  # noqa: E501
                )
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    utils.load_env()
    logging_config.configure_logging()

    bootstrap_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC")
    group_id = os.environ.get("CONSUMER_GROUP_ID", "consumer-group-id")
    schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    schema_type = "AVRO"

    with open("./schemas/schema.avsc") as avro_schema_file:
        avro_schema = avro_schema_file.read()
    schema_client = SchemaClient(schema_registry_url, topic, avro_schema, schema_type)

    # Schema already in Schema Registry, So fetch from Schema Registry
    schema_str = schema_client.get_schema_str()
    consumer = AvroConsumerClass(
        bootstrap_server,
        topic,
        group_id,
        schema_client.schema_registry_client,
        schema_str,
    )
    consumer.consume_messages()
