
import os
import json
import time
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from confluent_kafka import Consumer


KAFKA_ADDRESS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
GROUP = os.getenv("KAFKA_GROUP", "picker-group")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "picker-1")

history = deque(maxlen=100)
history_lock = threading.Lock()
stop_event = threading.Event()


def read_orders():
    # Каждый экземпляр имеет собственный Consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_ADDRESS,
        "group.id": GROUP,
        "client.id": INSTANCE_NAME,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC])
    print(f"{INSTANCE_NAME}: ожидаю заказы...", flush=True)

    try:
        while not stop_event.is_set():
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}", flush=True)
                continue

            try:
                order = json.loads(
                    message.value().decode("utf-8")
                )

                print(
                    f"Собираю заказ {order['order_id']}",
                    flush=True
                )

                # Имитируем работу сотрудника склада
                time.sleep(2)

                # Подтверждаем обработанное сообщение
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                result = {
                    "order_id": order["order_id"],
                    "items": order["items"],
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "instance": INSTANCE_NAME,
                    "status": "Собран",
                    "processed_at": datetime.now(
                        timezone.utc
                    ).isoformat()
                }

                with history_lock:
                    history.appendleft(result)

                print(
                    f"Заказ {order['order_id']} собран!",
                    flush=True
                )

            except Exception as error:
                print(f"Ошибка обработки: {error}", flush=True)

    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event.clear()

    worker = threading.Thread(
        target=read_orders,
        daemon=True
    )
    worker.start()

    yield

    stop_event.set()
    worker.join(timeout=5)


app = FastAPI(
    title="Picker Service",
    lifespan=lifespan
)


@app.get("/")
def index():
    return FileResponse(
        Path(__file__).with_name("index.html")
    )


@app.get("/api/orders")
def get_orders():
    with history_lock:
        return list(history)


@app.get("/api/info")
def get_info():
    return {
        "instance": INSTANCE_NAME,
        "group": GROUP,
        "topic": TOPIC
    }
