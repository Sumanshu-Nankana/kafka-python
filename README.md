# Learn Kafka with Python


### Start the Redpanda Cluster with Single Node

```
docker-compose -f docker-compose-1-node.yaml down
```

### Start the Redpanda Cluster with Three Node

```
docker-compose -f docker-compose-3-nodes.yaml down
```

### Run the Producer (_Make Sure Redpanda Cluster is up_.)

```
python src\producer.py
```