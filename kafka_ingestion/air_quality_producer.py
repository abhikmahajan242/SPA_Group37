"""
UrbanPulse - Air Quality Kafka Producer

Task B - Problem Statement 5

Features:
- Reads Air Quality data from JSONL dataset
- Sends records to Kafka topic: air_quality
- Uses sensor_id as Kafka message key
- Implements retry logic (at-least-once semantics)
- Simulates 5% NULL AQI sensor failures
- Simulates live streaming
"""

import json
import time
import random
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

TOPIC_NAME = "air_quality"

BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094"
]

MAX_RETRIES = 3

# Development Settings
MAX_RECORDS = 300
STREAM_DELAY = 0.01


# -------------------------------------------------------
# Dataset Location
# -------------------------------------------------------

DATA_FILE = (
    Path(__file__).resolve().parent.parent
    / "data_generators"
    / "urbanpulse_air_quality.jsonl"
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

    sensor_key = record["sensor_id"]

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            future = producer.send(
                TOPIC_NAME,
                key=sensor_key,
                value=record
            )

            future.get(timeout=10)

            logger.info(
                "Sent Sensor=%s AQI=%s",
                record["sensor_id"],
                record["aqi"]
            )

            return True

        except KafkaError as error:

            logger.warning(
                "Attempt %s/%s failed for Sensor=%s : %s",
                attempt,
                MAX_RETRIES,
                record["sensor_id"],
                error
            )

            time.sleep(2)

    logger.error(
        "Failed after %s retries : %s",
        MAX_RETRIES,
        record["sensor_id"]
    )

    return False


# -------------------------------------------------------
# Main Function
# -------------------------------------------------------

def main():

    logger.info("Starting Air Quality Producer...")

    with open(DATA_FILE, "r") as file:

        for index, line in enumerate(file):

            if index >= MAX_RECORDS:
                break

            record = json.loads(line)

            # Simulate 5% sensor failure (NULL AQI)
            if random.random() < 0.05:
                logger.warning(
                    "Simulated sensor failure: NULL AQI for Sensor=%s",
                    record["sensor_id"]
                )
                record["aqi"] = None

            send_record(record)

            time.sleep(STREAM_DELAY)

    producer.flush()
    producer.close()

    logger.info("Air Quality Producer Finished Successfully.")


# -------------------------------------------------------

if __name__ == "__main__":
    main()