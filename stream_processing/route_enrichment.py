import csv
import json
import logging

from kafka import KafkaConsumer, KafkaProducer

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Load Route Schedule CSV
# -----------------------------
route_lookup = {}

with open("../data_generators/route_schedule.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        route_lookup[row["route_id"]] = {
            "route_name": row["route_name"],
            "terminal": row["terminal"],
            "scheduled_arrival_time": row["scheduled_arrival_time"]
        }

logging.info(f"Loaded {len(route_lookup)} routes from CSV.")

# -----------------------------
# Kafka Consumer
# -----------------------------
consumer = KafkaConsumer(
    "bus_gps",
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# -----------------------------
# Kafka Producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

logging.info("Route Enrichment Started...")

# -----------------------------
# Stream Processing
# -----------------------------
try:

    for message in consumer:

        bus = message.value

        route_id = bus["route_id"]

        route = route_lookup.get(route_id)

        if route is None:
            continue

        enriched = {
            **bus,
            "route_name": route["route_name"],
            "terminal": route["terminal"],
            "scheduled_arrival_time": route["scheduled_arrival_time"]
        }

        producer.send(
            "bus_gps_enriched",
            value=enriched
        )

        logging.info(
            f"Bus={bus['bus_id']} | "
            f"{route['route_name']} | "
            f"{route['terminal']} | "
            f"ETA={route['scheduled_arrival_time']}"
        )

except KeyboardInterrupt:
    logging.info("Stopping Route Enrichment...")

finally:
    producer.flush()
    producer.close()
    consumer.close()