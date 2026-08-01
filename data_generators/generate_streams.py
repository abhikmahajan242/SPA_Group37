#!/usr/bin/env python3
"""
UrbanPulse Test Data Generator

Continuous streaming mode (default) sends ~30 events/sec until Ctrl+C.
One-shot mode (--count N) sends N batches and exits.

Usage:
    python3 generate_streams.py                            # continuous, random data
    python3 generate_streams.py --triggers                 # continuous + trigger sensors
    python3 generate_streams.py --count 50                 # one-shot, 50 batches
    python3 generate_streams.py --count 50 --triggers      # one-shot + triggers
    python3 generate_streams.py --to-files                 # write JSONL files instead
"""

import argparse
import json
import random
import signal
import sys
import time as time_module
from datetime import datetime, timezone

# ──────────────────────────────────────────────
# Kafka (optional — only imported if --to-files is not set)
# ──────────────────────────────────────────────
try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
TARGET_EVENTS_PER_SEC = 30

TOPICS = {
    "bus": "urbanpulse.bus_gps",
    "traffic": "urbanpulse.traffic_signals",
    "aqi": "urbanpulse.air_quality",
    "meter": "urbanpulse.smart_meters",
}

WARDS = ["WARD-1", "WARD-2", "WARD-3", "WARD-4", "WARD-5"]

ZONES = ["Cyber Hub", "Golf Course Road", "Sector 48", "Sector 65", "Sector 68", "Sector 70"]
ROUTES = ["R-101", "R-102", "R-103", "R-104", "R-105"]
SIGNAL_PHASES = ["RED", "GREEN", "YELLOW"]

# Trigger sensor IDs (used when --triggers is set)
TRIGGER_AQI_SENSOR = "AQI-1"
TRIGGER_JUNCTION = "JUNC-99"
TRIGGER_ZONE = "Sector 48"
TRIGGER_ROUTE = "R-101"
TRIGGER_BUS_A = "BUS-777"
TRIGGER_BUS_B = "BUS-778"

running = True


def handle_signal(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def format_ts(dt: datetime) -> str:
    """ISO-8601 with Z suffix, compatible with Java Instant.parse()."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond:06d}Z"


def haversine_distance(lat1, lon1, lat2, lon2):
    """Approximate meters between two lat/lon points (for verifying bunching distances)."""
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    return ((dlat * 111320) ** 2 + (dlon * 111320 * 0.88) ** 2) ** 0.5


# ──────────────────────────────────────────────
# Record generators
# ──────────────────────────────────────────────

def random_bus(ts: str) -> dict:
    return {
        "bus_id": f"BUS-{random.randint(1000, 1999)}",
        "route_id": random.choice(ROUTES),
        "lat": round(random.uniform(28.35, 28.50), 6),
        "lon": round(random.uniform(76.95, 77.10), 6),
        "speed_kmh": random.randint(0, 65),
        "occupancy_pct": random.randint(10, 100),
        "timestamp": ts,
    }


def random_traffic(ts: str) -> dict:
    return {
        "junction_id": f"JUNC-{random.randint(100, 500)}",
        "zone": random.choice(ZONES),
        "vehicle_count": random.randint(0, 150),
        "avg_wait_sec": random.randint(10, 240),
        "signal_phase": random.choice(SIGNAL_PHASES),
        "timestamp": ts,
    }


def random_aqi(ts: str) -> dict:
    pm25 = random.randint(40, 350)
    aqi_val = None if random.random() <= 0.05 else pm25 + random.randint(10, 50)
    return {
        "sensor_id": f"AQI-{random.randint(1, 100)}",
        "zone": random.choice(ZONES),
        "pm25": pm25,
        "pm10": pm25 + random.randint(20, 80),
        "aqi": aqi_val,
        "timestamp": ts,
    }


def random_smart_meter(ts: str) -> dict:
    return {
        "meter_id": f"SM-{random.randint(1000, 9999)}",
        "ward_id": random.choice(WARDS),
        "kwh_reading": round(random.uniform(0.5, 5.0), 2),
        "voltage": round(random.uniform(220, 240), 1),
        "power_factor": round(random.uniform(0.7, 1.0), 2),
        "timestamp": ts,
    }


# ──────────────────────────────────────────────
# Trigger record generators
# ──────────────────────────────────────────────

def trigger_aqi(ts: str) -> dict:
    """AQI > 300 for the trigger sensor (AQI-1 in Cyber Hub)."""
    return {
        "sensor_id": TRIGGER_AQI_SENSOR,
        "zone": "Cyber Hub",
        "pm25": 300,
        "pm10": 420,
        "aqi": random.randint(310, 400),
        "timestamp": ts,
    }


def trigger_traffic(ts: str) -> dict:
    """High wait time for the trigger junction (JUNC-99 in Sector 48)."""
    return {
        "junction_id": TRIGGER_JUNCTION,
        "zone": TRIGGER_ZONE,
        "vehicle_count": 120,
        "avg_wait_sec": round(random.uniform(190, 240), 1),
        "signal_phase": "RED",
        "timestamp": ts,
    }


def trigger_bus_bunching(ts: str) -> tuple:
    """Two buses on the same route within ~50m of each other (busy bus stop area)."""
    bunch_lat = 28.40 + random.uniform(-0.0002, 0.0002)
    bunch_lon = 77.00 + random.uniform(-0.0002, 0.0002)
    bus_a = {
        "bus_id": TRIGGER_BUS_A,
        "route_id": TRIGGER_ROUTE,
        "lat": round(bunch_lat, 6),
        "lon": round(bunch_lon, 6),
        "speed_kmh": round(random.uniform(5, 20), 1),
        "occupancy_pct": random.randint(60, 90),
        "timestamp": ts,
    }
    bus_b = {
        "bus_id": TRIGGER_BUS_B,
        "route_id": TRIGGER_ROUTE,
        "lat": round(bunch_lat + 0.0003, 6),
        "lon": round(bunch_lon + 0.0003, 6),
        "speed_kmh": round(random.uniform(5, 20), 1),
        "occupancy_pct": random.randint(60, 90),
        "timestamp": ts,
    }
    return bus_a, bus_b


# ──────────────────────────────────────────────
# Sender
# ──────────────────────────────────────────────

def send(producer, topic: str, record: dict):
    if producer:
        producer.send(topic, value=json.dumps(record).encode("utf-8"))
    else:
        # Handle not implemented for continuous file mode — too many open files
        pass


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="UrbanPulse test data generator")
    parser.add_argument("--triggers", action="store_true",
                        help="Include trigger sensor data for all 3 detectors")
    parser.add_argument("--to-files", action="store_true",
                        help="Write JSONL files instead of producing to Kafka")
    parser.add_argument("--count", type=int, default=None,
                        help="Number of batches to send (default: continuous until Ctrl+C)")
    args = parser.parse_args()

    producer = None
    if not args.to_files:
        if not HAS_KAFKA:
            print("ERROR: kafka-python is not installed. Run: pip install kafka-python")
            sys.exit(1)
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)  # type: ignore
        print(f"Producing to Kafka at {BOOTSTRAP_SERVERS}")
        print(f"  Topics: {list(TOPICS.values())}")
    else:
        print("--to-files not supported in continuous mode. Use --count instead.")
        sys.exit(1)

    # Each tick sends one record to each of 4 topics = 4 events
    # To hit 30 events/sec, tick every ~130ms
    tick_interval = 4.0 / TARGET_EVENTS_PER_SEC

    batch_count = 0
    trigger_cycle = 0
    total_events = 0
    start_time = time_module.time()

    print(f"\nStreaming at ~{TARGET_EVENTS_PER_SEC} events/sec ({TARGET_EVENTS_PER_SEC // 3} batches/sec)")
    if args.triggers:
        print("  Trigger sensors active:")
        print(f"    AQI:       {TRIGGER_AQI_SENSOR} (aqi > 300, in Cyber Hub)")
        print(f"    Gridlock:  {TRIGGER_JUNCTION} (avgWaitSec > 180, in {TRIGGER_ZONE})")
        print(f"    Bunching:  {TRIGGER_BUS_A} + {TRIGGER_BUS_B} (within 50m on {TRIGGER_ROUTE})")
    print("  Press Ctrl+C to stop.\n")

    try:
        while running:
            if args.count is not None and batch_count >= args.count:
                break

            now = format_ts(datetime.now(timezone.utc))

            # ── Bus GPS: random + optional bunching trigger ──
            bus_records = [random_bus(now)]
            if args.triggers:
                ba, bb = trigger_bus_bunching(now)
                bus_records.append(ba)
                bus_records.append(bb)

            # ── Traffic: random + optional gridlock trigger ──
            traffic_records = [random_traffic(now)]
            if args.triggers:
                traffic_records.append(trigger_traffic(now))

            # ── AQI: random + optional emergency trigger (every 5th cycle) ──
            aqi_records = [random_aqi(now)]
            if args.triggers and trigger_cycle % 5 == 0:
                aqi_records.append(trigger_aqi(now))

            # ── Smart Meter: random ──
            meter_records = [random_smart_meter(now)]

            # Send all records
            for rec in bus_records:
                send(producer, TOPICS["bus"], rec)
            for rec in traffic_records:
                send(producer, TOPICS["traffic"], rec)
            for rec in aqi_records:
                send(producer, TOPICS["aqi"], rec)
            for rec in meter_records:
                send(producer, TOPICS["meter"], rec)

            events_this_tick = len(bus_records) + len(traffic_records) + len(aqi_records) + len(meter_records)
            total_events += events_this_tick
            batch_count += 1
            trigger_cycle += 1

            # Print periodic status
            if batch_count % 100 == 0:
                elapsed = time_module.time() - start_time
                rate = total_events / elapsed if elapsed > 0 else 0
                print(f"  {total_events} events sent ({rate:.0f}/sec) — running for {elapsed:.0f}s")

            time_module.sleep(tick_interval)

    finally:
        if producer:
            producer.flush()
            producer.close()

    elapsed = time_module.time() - start_time
    rate = total_events / elapsed if elapsed > 0 else 0
    print(f"\nDone. Sent {total_events} events in {elapsed:.1f}s ({rate:.0f}/sec).")


if __name__ == "__main__":
    main()
