import json
import os

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-tracker")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}


def main():
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC])

    print(f"Consumer is running and subscribed to '{TOPIC}' topic")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            value = msg.value().decode("utf-8")
            order = json.loads(value)
            print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")
    except KeyboardInterrupt:
        print("\nStopping consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
