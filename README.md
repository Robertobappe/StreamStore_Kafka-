# StreamStore Kafka

A simple order streaming system using **Apache Kafka** with Python. A producer sends orders to a Kafka topic and a consumer (tracker) reads and displays them in real time.

## Architecture

```
[producer.py] ---> [Kafka Broker (Docker)] ---> [tracker.py]
                    Topic: "orders"
```

- **producer.py** - Generates an order (with UUID, user, item, quantity) and publishes it to the `orders` topic
- **tracker.py** - Subscribes to the `orders` topic and prints received orders in real time

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.8+

## Getting Started

### 1. Start Kafka

```bash
docker compose up -d
```

Wait a few seconds for Kafka to initialize.

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the consumer (tracker)

In one terminal:

```bash
python tracker.py
```

### 4. Send an order (producer)

In another terminal:

```bash
python producer.py
```

You should see the order appear in the tracker terminal.

## Configuration

The following environment variables can be used to configure the scripts:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `orders` | Topic name for orders |
| `KAFKA_GROUP_ID` | `order-tracker` | Consumer group ID (tracker only) |

## Useful Kafka Commands

Validate that the topic was created:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Describe the topic and see its partitions:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

View all events in a topic:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
```

## License

[MIT](LICENSE)

