import json
import logging
import os

import jsonschema

import logging_config
import utils
from admin import Admin
from producer import ProducerClass

schema = {
    "type": "object",
    "properties": {
        "first_name": {
            "type": "string",
        },
        "middle_name": {"type": "string"},
        "last_name": {"type": "string"},
        "age": {
            "type": "integer",
            "minimum": 0,
        },
    },
}


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
    def __init__(self, bootstrap_server, topic, schema):
        super().__init__(bootstrap_server, topic)
        self.schema = schema
        self.value_serializer = lambda v: json.dumps(v).encode("utf-8")

    def send_message(self, message_dict):
        try:
            # Validate message against JSON schema
            jsonschema.validate(message_dict, self.schema)
            logging.info("Schema Validated")

            # Convert message to JSON string and serialize
            message_json = self.value_serializer(message_dict)
            self.producer.produce(self.topic, message_json)
            logging.info(f"Message Sent: {message_json}")
        except jsonschema.ValidationError as e:
            logging.error(f"Validation Error: {e}")
            return
        except Exception as e:
            logging.error(f"Error sending message: {e}")


if __name__ == "__main__":
    utils.load_env()
    logging_config.configure_logging()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC")

    admin = Admin(bootstrap_servers)
    producer = JSONProducer(bootstrap_servers, topic, schema)
    admin.create_topic(topic)

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
