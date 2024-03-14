"""The Producer code will manually validate the schema."""
import logging
import os

from confluent_kafka import Producer

import logging_config
import utils
from admin import Admin


class ProducerClass:
    """Producer class for sending messages to Kafka."""

    def __init__(
        self,
        bootstrap_servers,
        topic,
        compression_type=None,
        message_size=None,
        batch_size=None,
        waiting_time=None,
    ):
        """Initializes the producer."""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "partitioner": "random",
        }

        if compression_type:
            self.producer_conf["compression.type"] = compression_type
        if message_size:
            self.producer_conf["message.max.bytes"] = message_size
        if batch_size:
            self.producer_conf["batch.size"] = batch_size
        if waiting_time:
            self.producer_conf["linger.ms"] = waiting_time

        self.producer = Producer(self.producer_conf)

    def send_message(self, message):
        """Sends a message to Kafka.

        Args:
            message (str): Message to send.
        """
        try:
            self.producer.produce(self.topic, message)
            # self.producer.flush()
            logging.info(f"Message sent to topic {self.topic}: {message}")
        except Exception as e:
            logging.error(f"Error sending message: {e}")

    def commit(self):
        """Commit the message."""
        self.producer.flush()
        logging.info("Messages committed to Kafka.")


if __name__ == "__main__":
    utils.load_env()
    logging_config.configure_logging()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC")
    schema_url = os.environ.get("SCHEMA_URL")

    admin = Admin(bootstrap_servers)
    producer = ProducerClass(bootstrap_servers, topic)
    admin.create_topic(topic)

    try:
        while True:
            message = input("Enter any message: ")
            producer.send_message(message)
    except KeyboardInterrupt:
        pass

    producer.commit()
