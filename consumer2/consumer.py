import json
import os
import time

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "Topic2")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer-group")


def build_consumer(max_attempts: int = 20, pause_seconds: float = 3.0) -> KafkaConsumer:
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS.split(",")],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                key_deserializer=lambda value: value.decode("utf-8") if value else None,
            )
            print(f"[CONSUMER:{TOPIC}] Connected to Kafka on attempt {attempt}.")
            return consumer
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"[CONSUMER:{TOPIC}] Kafka connection failed (attempt {attempt}/{max_attempts}): {exc}")
            time.sleep(pause_seconds)

    raise RuntimeError(f"Could not connect to Kafka after {max_attempts} attempts: {last_error}")


def consume() -> None:
    consumer = build_consumer()
    print(f"[CONSUMER:{TOPIC}] Waiting for messages... group_id={GROUP_ID}")

    for message in consumer:
        print(
            f"[CONSUMER:{TOPIC}] partition={message.partition} offset={message.offset} "
            f"key={message.key} value={message.value}"
        )


if __name__ == "__main__":
    consume()
