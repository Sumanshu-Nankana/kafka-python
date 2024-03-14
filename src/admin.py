import logging

from confluent_kafka.admin import AdminClient, NewTopic


class Admin:
    """Admin class for managing Kafka topics."""

    def __init__(self, bootstrap_servers):
        """Initializes the AdminClient."""
        self.bootstrap_servers = bootstrap_servers
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})

    def topic_exists(self, topic_name):
        """Checks if a topic exists.

        Args:
            topic_name (str): Name of the topic to check.

        Returns:
            bool: True if the topic exists, False otherwise.
        """
        all_topics = self.admin.list_topics()
        return topic_name in all_topics.topics.keys()

    def create_topic(self, topic_name, partitions=1):
        """Creates a new topic.

        Args:
            topic_name (str): Name of the topic to create.
        """
        if not self.topic_exists(topic_name):
            new_topic = NewTopic(topic_name, num_partitions=partitions)
            self.admin.create_topics([new_topic])
            logging.info(f"Topic {topic_name} created")
        else:
            logging.info(f"Topic {topic_name} already exists")
