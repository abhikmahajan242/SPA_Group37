import json
import logging

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Consumer
consumer = KafkaConsumer(
    "bus_gps",
    "air_quality",
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Producer (writes to DLQ)
producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logging.info("DLQ Consumer Started...")

for message in consumer:

    topic = message.topic
    record = message.value

    error_reason = None

    # -----------------------
    # Air Quality Validation
    # -----------------------
    if topic == "air_quality":

        aqi = record.get("aqi")

        if aqi is None:
            error_reason = "Null AQI"

        elif aqi > 500:
            error_reason = "AQI Out of Range"

    # -----------------------
    # Bus GPS Validation
    # -----------------------
    elif topic == "bus_gps":

        lat = record.get("lat")
        lon = record.get("lon")

        if lat is None or lon is None:
            error_reason = "Missing Coordinates"

        elif not (-90 <= lat <= 90):
            error_reason = "Invalid Latitude"

        elif not (-180 <= lon <= 180):
            error_reason = "Invalid Longitude"

    # -----------------------
    # Send to DLQ
    # -----------------------
    if error_reason:

        dlq_message = {
            "source_topic": topic,
            "error_reason": error_reason,
            "original_record": record
        }

        producer.send(
            "urbanpulse.dlq",
            value=dlq_message
        )

        producer.flush()

        logging.warning(
            f"DLQ -> {topic} | {error_reason}"
        )