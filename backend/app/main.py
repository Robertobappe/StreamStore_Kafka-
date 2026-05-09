import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "dashboard-consumer")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

app = FastAPI(title="StreamStore Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

orders: list[dict] = []
stats = {
    "total_orders": 0,
    "total_items": 0,
    "total_revenue": 0.0,
    "orders_per_product": defaultdict(int),
    "orders_per_user": defaultdict(int),
    "revenue_per_product": defaultdict(float),
    "orders_timeline": [],
}

PRICE_MAP = {
    "frozen yogurt": 5.99,
    "açaí bowl": 8.99,
    "smoothie": 6.49,
    "milkshake": 7.49,
    "ice cream": 4.99,
    "sorbet": 5.49,
    "gelato": 6.99,
    "popsicle": 2.99,
    "frozen fruit": 3.99,
    "iced coffee": 4.49,
}

DEFAULT_PRICE = 5.00

connected_clients: list[WebSocket] = []


def process_order(order: dict):
    item = order.get("item", "unknown")
    quantity = order.get("quantity", 1)
    price = PRICE_MAP.get(item, DEFAULT_PRICE)
    revenue = price * quantity

    order["price"] = price
    order["revenue"] = revenue
    order["timestamp"] = datetime.now(timezone.utc).isoformat()

    orders.append(order)

    stats["total_orders"] += 1
    stats["total_items"] += quantity
    stats["total_revenue"] += revenue
    stats["orders_per_product"][item] += quantity
    stats["orders_per_user"][order.get("user", "unknown")] += 1
    stats["revenue_per_product"][item] += revenue

    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    timeline = stats["orders_timeline"]
    if timeline and timeline[-1]["time"] == now:
        timeline[-1]["count"] += 1
        timeline[-1]["revenue"] += revenue
    else:
        timeline.append({"time": now, "count": 1, "revenue": revenue})

    if len(timeline) > 60:
        stats["orders_timeline"] = timeline[-60:]

    return order


async def broadcast(message: dict):
    disconnected = []
    for ws in connected_clients:
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        connected_clients.remove(ws)


async def kafka_consumer_loop():
    await asyncio.sleep(3)

    consumer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}")
                continue

            value = msg.value().decode("utf-8")
            order = json.loads(value)
            processed = process_order(order)
            await broadcast({
                "type": "new_order",
                "order": processed,
                "stats": get_stats_dict(),
            })
            await asyncio.sleep(0.05)
    except Exception as e:
        print(f"Kafka consumer error: {e}")
    finally:
        consumer.close()


def get_stats_dict():
    return {
        "total_orders": stats["total_orders"],
        "total_items": stats["total_items"],
        "total_revenue": round(stats["total_revenue"], 2),
        "orders_per_product": dict(stats["orders_per_product"]),
        "orders_per_user": dict(stats["orders_per_user"]),
        "revenue_per_product": {
            k: round(v, 2) for k, v in stats["revenue_per_product"].items()
        },
        "orders_timeline": stats["orders_timeline"],
    }


@app.on_event("startup")
async def startup():
    asyncio.create_task(kafka_consumer_loop())


@app.get("/api/stats")
async def api_stats():
    return get_stats_dict()


@app.get("/api/orders")
async def api_orders(limit: int = 50):
    return orders[-limit:][::-1]


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)
    try:
        await ws.send_json({
            "type": "init",
            "orders": orders[-50:][::-1],
            "stats": get_stats_dict(),
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in connected_clients:
            connected_clients.remove(ws)


FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "frontend")


@app.get("/")
async def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


app.mount("/css", StaticFiles(directory=os.path.join(FRONTEND_DIR, "css")), name="css")
app.mount("/js", StaticFiles(directory=os.path.join(FRONTEND_DIR, "js")), name="js")
