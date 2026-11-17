
import json
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

TOPIC = "campus-events"
DEPARTMENTS = ["Computer Science", "Mathematics", "Physics"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

base = datetime.now(timezone.utc).replace(microsecond=0)

try:
    start_time = time.time()

    # for event_id in range(1, 101):
    for event_id in range(1, 201):
        event_time = base + timedelta(seconds=event_id)

        event = {
            "eventId": event_id,
            "studentId": f"S{random.randint(100, 999)}",
            "eventType": random.choice(["check-in", "open-course"]),
            "department": random.choice(DEPARTMENTS),
            "eventTime": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        producer.send(TOPIC, value=event)

    producer.flush()

    elapsed = time.time() - start_time
    ingestion_rate = 200 / elapsed
    # ingestion_rate = 100 / elapsed

    print("Total events sent:", 200)
    # print("Total events sent:", 100)
    print("Total sending time:", round(elapsed, 2), "seconds")
    print("Ingestion rate:", round(ingestion_rate, 2), "events/second")

finally:
    producer.close()
