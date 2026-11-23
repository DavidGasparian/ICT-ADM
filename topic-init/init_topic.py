
import os
import time

from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    NewPartitions,
    ConfigResource,
    ConfigEntry,
    AlterConfigOpType,
    ResourceType
)


BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"
)
TOPIC = os.getenv("TOPIC_NAME", "orders")
PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "2"))
RETENTION = os.getenv(
    "TOPIC_RETENTION_MS", "604800000"
)

admin = AdminClient({
    "bootstrap.servers": BROKER
})


# Дожидаемся готовности Kafka
for attempt in range(60):
    try:
        metadata = admin.list_topics(timeout=5)
        break
    except Exception:
        print("Ожидаем Kafka...", flush=True)
        time.sleep(2)
else:
    raise RuntimeError("Kafka не запустилась")


# Проверяем, существует ли топик
if TOPIC not in metadata.topics:
    topic = NewTopic(
        TOPIC,
        num_partitions=PARTITIONS,
        replication_factor=1,
        config={"retention.ms": RETENTION}
    )

    admin.create_topics([topic])[TOPIC].result()

    print(f"Топик {TOPIC} создан", flush=True)

else:
    current = len(metadata.topics[TOPIC].partitions)

    if PARTITIONS > current:
        admin.create_partitions([
            NewPartitions(TOPIC, PARTITIONS)
        ])[TOPIC].result()

        print(
            f"Партиций стало: {PARTITIONS}",
            flush=True
        )

    elif PARTITIONS < current:
        raise RuntimeError(
            "Kafka не поддерживает уменьшение "
            "количества партиций существующего топика"
        )

    # Обновляем retention
    resource = ConfigResource(ResourceType.TOPIC, TOPIC)

    resource.add_incremental_config(
        ConfigEntry(
            "retention.ms",
            RETENTION,
            incremental_operation=AlterConfigOpType.SET
        )
    )

    futures = admin.incremental_alter_configs([resource])

    for future in futures.values():
        future.result()

print("Конфигурация топика применена!", flush=True)
