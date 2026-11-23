
import os
import json
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from confluent_kafka import Producer, KafkaException


# Настройки берём из переменных окружения
KAFKA_ADDRESS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "user-1")

app = FastAPI(title="User Service")

# Producer отвечает за отправку сообщений в Kafka
producer = Producer({
    "bootstrap.servers": KAFKA_ADDRESS,
    "client.id": INSTANCE_NAME,
})

# Последние 30 отправленных заказов
history = deque(maxlen=30)


# Формат данных, которые получаем от сайта
class OrderRequest(BaseModel):
    items: list[str] = Field(min_length=1)


# Главная страница
@app.get("/")
def index():
    return FileResponse(
        Path(__file__).with_name("index.html")
    )


# Получение истории заказов
@app.get("/api/orders")
def get_orders():
    return list(history)


# Создание и отправка нового заказа
@app.post("/api/orders")
def create_order(order: OrderRequest):
    order_id = uuid.uuid4().hex[:8]

    event = {
        "event_type": "order_created",
        "order_id": order_id,
        "items": order.items,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    delivery = {}

    # Kafka вызывает эту функцию после доставки
    def on_delivery(error, message):
        delivery["error"] = error
        if error is None:
            delivery["partition"] = message.partition()
            delivery["offset"] = message.offset()

    try:
        producer.produce(
            topic=TOPIC,
            key=order_id,
            value=json.dumps(
                event, ensure_ascii=False
            ).encode("utf-8"),
            on_delivery=on_delivery,
        )

        # Ждём подтверждение Kafka, максимум 10 секунд
        remaining = producer.flush(10)

    except (KafkaException, BufferError) as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Ошибка Kafka: {exc}"
        )

    if remaining or delivery.get("error"):
        raise HTTPException(
            status_code=503,
            detail="Kafka не подтвердила отправку заказа"
        )

    result = {
        **event,
        "partition": delivery["partition"],
        "offset": delivery["offset"],
    }

    history.appendleft(result)
    return result
