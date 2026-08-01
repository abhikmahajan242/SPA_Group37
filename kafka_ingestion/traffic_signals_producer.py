import json
import time
import logging

from kafka import KafkaProducer

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# Input File
DATA_FILE = "../data_generators/urbanpulse_traffic_signals.jsonl"

try:

    with open(DATA_FILE, "r") as file:

        count = 0

        for line in file:

            event = json.loads(line)

            # Use junction_id as the Kafka message key
            key = event["junction_id"]

            future = producer.send(
                "traffic_signals",
                key=key,
                value=event
            )

            # Wait for acknowledgement (at-least-once semantics)
            future.get(timeout=10)

            logging.info(
                f"Sent Junction={event['junction_id']} | "
                f"Phase={event['signal_phase']} | "
                f"Vehicles={event['vehicle_count']}"
            )

            count += 1

            # Send only the first 300 records
            if count >= 300:
                break

            # Small delay to simulate real-time streaming
            time.sleep(0.01)

except Exception as e:
    logging.error(f"Error while sending messages: {e}")

finally:
    producer.flush()
    producer.close()

logging.info("Traffic Signal Producer Finished Successfully.")