import json
from collections import Counter

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "urbanpulse.dlq",
    bootstrap_servers=[
        "localhost:9092",
        "localhost:9093",
        "localhost:9094"
    ],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

error_counter = Counter()

for message in consumer:
    record = message.value
    error = record.get("error_reason", "Unknown Error")
    error_counter[error] += 1

total = sum(error_counter.values())

print("\n")
print("=" * 45)
print("      DLQ ERROR TYPE DISTRIBUTION")
print("=" * 45)
print(f"{'Error Type':<30}{'Count'}")
print("-" * 45)

for error, count in error_counter.items():
    print(f"{error:<30}{count}")

print("-" * 45)
print(f"{'Total DLQ Messages':<30}{total}")
print("=" * 45)