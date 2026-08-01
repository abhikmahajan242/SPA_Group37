import json
import time
import sys
import logging

from kafka import KafkaConsumer

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Read command-line argument
# -----------------------------
if len(sys.argv) != 2:
    print("Usage:")
    print("python priority_consumers.py high")
    print("python priority_consumers.py standard")
    sys.exit(1)

mode = sys.argv[1].lower()

if mode == "high":
    group_id = "HIGH_PRIORITY"
    processing_delay = 0
elif mode == "standard":
    group_id = "STANDARD_PRIORITY"
    processing_delay = 2
else:
    print("Invalid mode. Use 'high' or 'standard'")
    sys.exit(1)

logging.info(f"Starting {group_id} Consumer...")

consumer = KafkaConsumer(
    "traffic_signals",
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    group_id=group_id,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for message in consumer:

    event = message.value

    logging.info(
        f"[{group_id}] "
        f"Junction={event['junction_id']} | "
        f"Phase={event['signal_phase']} | "
        f"Vehicles={event['vehicle_count']}"
    )

    if processing_delay > 0:
        time.sleep(processing_delay)