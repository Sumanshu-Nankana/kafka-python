import logging
import os

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError


class SchemaClient:
    def __init__(self, schema_url, schema_subject_name, schema, schema_type):
        """Initialize the Schema Registry Client."""
        self.schema_url = schema_url
        self.schema_subject_name = schema_subject_name
        self.schema = schema
        self.schema_type = schema_type
        self.schema_registry_client = SchemaRegistryClient({"url": self.schema_url})

    def schema_exists_in_registry(self):
        try:
            self.schema_registry_client.get_latest_version(self.schema_subject_name)
            return True
        except SchemaRegistryError:
            return False

    def register_schema(self):
        """Register the Schema in Schema Registry."""
        if not self.schema_exists_in_registry():
            try:
                self.schema_registry_client.register_schema(
                    subject_name=self.schema_subject_name,
                    schema=Schema(self.schema, schema_type=self.schema_type),
                )
                logging.info(
                    f"Schema{self.schema} Registered for {self.schema_subject_name}"  # noqa: E501
                )
            except SchemaRegistryError as e:
                logging.error(f"Error while registering the Schema {e}")
                exit(1)
        else:
            logging.info(
                f"Schema: {self.schema} already exists in registry for {self.schema_subject_name}"  # noqa: E501
            )


if __name__ == "__main__":
    topic = os.environ.get("KAFKA_TOPIC")
    schema_url = os.environ.get("SCHEMA_URL")
    schema_type = "JSON"

    # In redpanda, JSON Schema not supported, but AVRO and PROTOBUF supported
    with open("./schemas/schema.json") as json_schema_file:
        json_schema = json_schema_file.read()

    schema_client = SchemaClient(schema_url, topic, json_schema, schema_type)
    schema_client.register_schema()
