import csv
import json
import os
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
TOPIC_1 = os.getenv("KAFKA_TOPIC_1", "Topic1")
TOPIC_2 = os.getenv("KAFKA_TOPIC_2", "Topic2")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/Divvy_Trips_2019_Q4.csv")
SEND_DELAY_SECONDS = float(os.getenv("SEND_DELAY_SECONDS", "0.0"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))


def build_producer(max_attempts: int = 20, pause_seconds: float = 3.0) -> KafkaProducer:
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS.split(",")],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks="all",
                retries=5,
            )
            print(f"[PRODUCER] Connected to Kafka on attempt {attempt}.")
            return producer
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"[PRODUCER] Kafka connection failed (attempt {attempt}/{max_attempts}): {exc}")
            time.sleep(pause_seconds)

    raise RuntimeError(f"Could not connect to Kafka after {max_attempts} attempts: {last_error}")


def produce_rows() -> None:
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    producer = build_producer()
    sent = 0

    with open(CSV_PATH, "r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader, start=1):
            event = {
                "event_id": row_number,
                "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "payload": row,
            }
            key = str(row_number)

            producer.send(TOPIC_1, key=key, value=event)
            producer.send(TOPIC_2, key=key, value=event)
            sent += 1

            print(f"[PRODUCER] Sent event_id={row_number} to {TOPIC_1} and {TOPIC_2}")

            if MAX_RECORDS > 0 and sent >= MAX_RECORDS:
                print(f"[PRODUCER] MAX_RECORDS={MAX_RECORDS} reached. Stopping producer.")
                break

            if SEND_DELAY_SECONDS > 0:
                time.sleep(SEND_DELAY_SECONDS)

    producer.flush()
    producer.close()
    print(f"[PRODUCER] Completed. Total events sent: {sent}")


if __name__ == "__main__":
    produce_rows()
