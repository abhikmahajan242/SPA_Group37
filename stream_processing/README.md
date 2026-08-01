# UrbanPulse Stream Processing

UrbanPulse is a local Docker-based stream-processing project for MetroConnect,
a fictitious smart-city platform. Kafka carries the events, Flink detects
incidents in real time, and Spark Structured Streaming produces analytics.

## Project Structure

```text
.
├── docker-compose.yml                    # Kafka, Flink, Spark, and Kafka UI
├── data_generators/generate_streams.py   # Continuous Kafka test-data generator
├── pom.xml                               # Flink Java build configuration
├── src/main/java/com/urbanpulse/
│   ├── UrbanPulseIncidentJob.java         # Flink entry point
│   ├── detectors/                         # AQI, gridlock, and bus-bunching logic
│   └── model/                             # Input events and alert POJOs
├── spark_jobs/
│   ├── ward_energy_summary.py             # Ward energy aggregation
│   └── health_advisories.py               # AQI Streaming SQL job
├── data/
│   ├── zone_profile.csv                   # Static Spark lookup table
│   ├── checkpoints/                       # Generated Spark recovery state (generated)
│   └── parquet/                           # Generated historical ward-energy data (generated)
└── submit_jobs.sh                         # Spark job submission helper
```

## Project Background

The platform consumes four JSON event streams, all using snake_case field names.

| Topic | Event purpose |
|---|---|
| `urbanpulse.bus_gps` | Bus location, route, speed, and occupancy readings |
| `urbanpulse.traffic_signals` | Junction vehicle-count and wait-time readings |
| `urbanpulse.air_quality` | Zone air-quality readings |
| `urbanpulse.smart_meters` | Ward electricity-meter readings |

### Flink Outputs

`urbanpulse.incidents` contains the following incident types:

- `AQI_EMERGENCY`: AQI exceeds 300.
- `GRIDLOCK`: a junction has average wait above 180 seconds for three cycles.
- `BUS_BUNCHING`: two buses on a route remain within 200m for over five minutes.

### Spark Outputs

- `ward_energy_summary`: 15-minute ward electricity aggregates.
- `urbanpulse.health_advisories`: enriched AQI advisories above the unhealthy threshold.
- `data/parquet/ward_energy/`: ward-energy historical data partitioned by ward and date.

## Flink - Setup

### 1. Start the local stack

```bash
docker compose up -d
docker compose ps
```

| Service | Address |
|---|---|
| Kafka from the host | `localhost:9092` |
| Kafka UI | http://localhost:8082 |
| Flink Web UI | http://localhost:8081 |
| Spark master Web UI | http://localhost:8083 |
| Spark worker Web UI | http://localhost:8084 |

### 2. Create Kafka topics

Create the complete set of source and output topics once Kafka is running.
The command is idempotent because `--if-not-exists` skips existing topics.

```bash
for topic in \
  urbanpulse.bus_gps \
  urbanpulse.traffic_signals \
  urbanpulse.air_quality \
  urbanpulse.smart_meters \
  urbanpulse.incidents \
  ward_energy_summary \
  urbanpulse.health_advisories; do
  docker exec kafka kafka-topics --create --if-not-exists \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
done
```

Verify the result:

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Install the host generator dependency

Only the Python generator runs on the host. PySpark runs inside Docker.

```bash
pip install kafka-python
```

### 4. Build the Flink job

Build with Maven so the JAR targets Java 17, matching the Flink container.

```bash
mvn clean package
```

This creates `target/urbanpulse-flink-1.0-SNAPSHOT.jar`. The `target/`
directory is mounted at `/opt/flink/usrlib`, so no manual copy is required.

## Flink - How to Run

### 1. Submit the incident-detection job

```bash
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.urbanpulse.UrbanPulseIncidentJob \
  /opt/flink/usrlib/urbanpulse-flink-1.0-SNAPSHOT.jar
```

Confirm that it is running:

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
```

### 2. Generate input events

Run the continuous generator with deterministic detector triggers:

```bash
python3 generate_streams.py --triggers
```

Stop it with `Ctrl+C`. For a finite test run, use:

```bash
python3 generate_streams.py --count 100 --triggers
```

### 3. Verify incident output

Use Kafka UI or consume the output topic directly:

```bash
docker exec kafka kafka-console-consumer \
  --topic urbanpulse.incidents \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

Look for `AQI_EMERGENCY`, `GRIDLOCK`, and `BUS_BUNCHING` alerts.

## Spark - Setup

Spark 3.5.3 runs in the `spark-master` and `spark-worker` containers.
PySpark is included in the Docker image; it does not need a host installation.

Spark uses the following mounted host directories:

- `data/zone_profile.csv`: static zone, population, and school-count lookup.
- `data/checkpoints/`: state and Kafka offsets used for restart recovery.
- `data/parquet/ward_energy/`: generated historical energy dataset.

Ensure Spark can write to the shared `data/` mount before submitting jobs:

```bash
mkdir -p data/checkpoints data/parquet
chmod -R 777 data
```

On its first run, `submit_jobs.sh` downloads the Spark 3.5.3 Kafka connector
and stores it in the container's Ivy cache.

## Spark - How to Run

### 1. Start event generation

In a separate terminal, start the generator before the Spark queries:

```bash
python3 generate_streams.py --triggers
```

### 2. Submit Spark jobs

Submit both queries:

```bash
bash submit_jobs.sh
```

Or submit one query at a time:

```bash
bash submit_jobs.sh ward_energy
bash submit_jobs.sh health
```

The ward-energy query uses a 45-minute watermark and calculates
`total_kwh_consumed`, `avg_power_factor`, and `peak_voltage` per `ward_id` and
15-minute tumbling window. It writes each micro-batch to Kafka and to Parquet
partitioned by `ward_id` and the event-time date.

The health-advisories query uses Spark SQL to compute a 10-minute AQI window
sliding every five minutes. It joins `zone_profile.csv`, keeps averages above
150, and writes updates to `urbanpulse.health_advisories`.

### 3. Verify Spark results

Check that both applications are running in the Spark master UI:
http://localhost:8083

Check ward-energy Kafka output:

```bash
docker exec kafka kafka-console-consumer \
  --topic ward_energy_summary \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 3
```

Expected fields: `ward_id`, `window`, `total_kwh_consumed`,
`avg_power_factor`, and `peak_voltage`.

Check enriched health advisories:

```bash
docker exec kafka kafka-console-consumer \
  --topic urbanpulse.health_advisories \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 3
```

Expected fields: `zone`, `window_end`, `rolling_avg_aqi`, `population`, and
`num_schools`. Each result must have `rolling_avg_aqi > 150`.

Check the required Parquet partition structure on the host:

```bash
find data/parquet/ward_energy -name '*.parquet'
```

Expected layout:

```text
data/parquet/ward_energy/ward_id=WARD-1/date=2026-08-01/part-...snappy.parquet
```
