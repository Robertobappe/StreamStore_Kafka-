import json
import os
import random
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

USERS = [
    "lara", "pedro", "ana", "carlos", "marina",
    "rafael", "julia", "lucas", "camila", "bruno",
    "fernanda", "diego", "beatriz", "thiago", "isabella",
]

ITEMS = [
    "frozen yogurt", "açaí bowl", "smoothie", "milkshake",
    "ice cream", "sorbet", "gelato", "popsicle",
    "frozen fruit", "iced coffee",
]

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user": random.choice(USERS),
        "item": random.choice(ITEMS),
        "quantity": random.randint(1, 5),
    }


def main():
    producer = Producer(producer_config)
    print(f"Simulator started — sending orders to '{TOPIC}'")
    print("Press Ctrl+C to stop\n")

    count = 0
    try:
        while True:
            order = generate_order()
            value = json.dumps(order).encode("utf-8")

            try:
                producer.produce(
                    topic=TOPIC,
                    value=value,
                    callback=delivery_report,
                )
            except BufferError:
                producer.flush()
                producer.produce(
                    topic=TOPIC,
                    value=value,
                    callback=delivery_report,
                )

            producer.poll(0)
            count += 1
            print(f"[{count}] {order['user']} ordered {order['quantity']}x {order['item']}")

            delay = random.uniform(0.5, 3.0)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nStopping simulator. Sent {count} orders.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
