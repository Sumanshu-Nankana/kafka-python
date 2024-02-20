import logging
import os

from confluent_kafka import Producer

import logging_config
import utils
from admin import Admin


class ProducerClass:
    """Producer class for sending messages to Kafka."""

    def __init__(self, bootstrap_servers, topic):
        """Initializes the producer"""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def send_message(self, message):
        """Sends a message to Kafka.

        Args:
            message (str): Message to send.
        """
        try:
            self.producer.produce(self.topic, message)
            self.producer.flush()
            logging.info(f"Message sent to topic {self.topic}: {message}")
        except Exception as e:
            logging.error(f"Error sending message: {e}")


if __name__ == "__main__":
    utils.load_env()
    logging_config.configure_logging()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC")

    admin = Admin(bootstrap_servers)
    producer = ProducerClass(bootstrap_servers, topic)

    if not admin.topic_exists(topic):
        admin.create_topic(topic)
    try:
        while True:
            message = input("Enter any message: ")
            producer.send_message(message)
    except KeyboardInterrupt:
        pass
