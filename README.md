# Learn Kafka with Python


### Start the Redpanda Cluster with Single Node

```
docker-compose -f docker-compose-1-node.yaml down
```

### Start the Redpanda Cluster with Three Node

```
docker-compose -f docker-compose-3-nodes.yaml down
```

### Browse the Redpanda Console UI
```commandline
http://localhost:8080/
```

### Run the Producer (_Make Sure Redpanda Cluster is up_.)

```
python src\producer.py
python src\avro_producer.py
```

### Run the Consumer (_Make Sure Redpanda Cluster is up_.)

```
python src\consumer.py
python src\avro_consumer.py
```

### The scripts in this directory provide various examples of using Confluent's Python client for Kafka:

* [Admin API](src/admin.py)
* [Schema Registry API](src/schema_registry_client.py)
* [String Producer](src/producer.py)
* [String Consumer](src/consumer.py)
* [Avro Producer](src/avro_producer.py)
* [Avro Consumer](src/avro_consumer.py)