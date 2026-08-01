"""
UrbanPulse - Bus GPS Kafka Producer

Task B - Problem Statement 5

Features:
- Reads Bus GPS data from JSONL dataset
- Sends records to Kafka topic: bus_gps
- Uses route_id as Kafka message key
- Implements retry logic (at-least-once semantics)
- Simulates live streaming
"""

import json
import time
import logging
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# -------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------
# Kafka Configuration
# -------------------------------------------------------

TOPIC_NAME = "bus_gps"

BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094"
]

MAX_RETRIES = 3

# Development Settings
MAX_RECORDS = 300       # Change to 5000 for full dataset if needed
STREAM_DELAY = 0.01     # 10 milliseconds


# -------------------------------------------------------
# Dataset Location
# -------------------------------------------------------

DATA_FILE = (
    Path(__file__).resolve().parent.parent
    / "data_generators"
    / "urbanpulse_bus_gps.jsonl"
)


# -------------------------------------------------------
# Create Kafka Producer
# -------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# -------------------------------------------------------
# Send Record with Retry Logic
# -------------------------------------------------------

def send_record(record):
    route_key = record["route_id"]

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            future = producer.send(
                TOPIC_NAME,
                key=route_key,
                value=record
            )

            # Wait for broker acknowledgement
            future.get(timeout=10)

            logger.info(
                "Sent Bus=%s Route=%s",
                record["bus_id"],
                route_key
            )

            return True

        except KafkaError as error:

            logger.warning(
                "Attempt %s/%s failed for Bus=%s : %s",
                attempt,
                MAX_RETRIES,
                record["bus_id"],
                error
            )

            time.sleep(2)

    logger.error(
        "Failed after %s retries : %s",
        MAX_RETRIES,
        record["bus_id"]
    )

    return False


# -------------------------------------------------------
# Main Function
# -------------------------------------------------------

def main():

    logger.info("Starting Bus GPS Producer...")

    with open(DATA_FILE, "r") as file:

        for index, line in enumerate(file):

            if index >= MAX_RECORDS:
                break

            record = json.loads(line)

            send_record(record)

            # Simulate live GPS stream
            time.sleep(STREAM_DELAY)

    producer.flush()
    producer.close()

    logger.info("Bus GPS Producer Finished Successfully.")


# -------------------------------------------------------

if __name__ == "__main__":
    main()