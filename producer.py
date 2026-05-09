import json
import os
import uuid

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS
}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")
        print(f"Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")


def main():
    producer = Producer(producer_config)

    order = {
        "order_id": str(uuid.uuid4()),
        "user": "lara",
        "item": "frozen yogurt",
        "quantity": 10
    }

    value = json.dumps(order).encode("utf-8")

    try:
        producer.produce(
            topic=TOPIC,
            value=value,
            callback=delivery_report
        )
    except BufferError:
        print(f"Producer queue is full ({len(producer)} messages pending). Waiting...")
        producer.flush()
        producer.produce(
            topic=TOPIC,
            value=value,
            callback=delivery_report
        )

    producer.flush()


if __name__ == "__main__":
    main()
