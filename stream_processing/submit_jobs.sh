#!/bin/bash
# UrbanPulse Spark Job Submission Script
# Usage: bash submit_jobs.sh [job_name]
#   job_name: "ward_energy", "health" (default: submit both)

set -e

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
MASTER="spark://spark-master:7077"
JOBS_DIR="/opt/spark/jobs"
DATA_DIR="/opt/spark/data"

SUBMIT="/opt/spark/bin/spark-submit"

submit_job() {
    local job_file=$1
    local job_name=$2

    echo "=========================================="
    echo "Submitting: $job_name ($job_file)"
    echo "=========================================="

    docker exec spark-master $SUBMIT \
        --master "$MASTER" \
        --packages "$PACKAGES" \
        --deploy-mode client \
        --name "$job_name" \
        --conf "spark.cores.max=2" \
        --conf "spark.jars.ivy=/tmp/ivy" \
        --conf "spark.sql.streaming.checkpointLocation=$DATA_DIR/checkpoints" \
        --conf "spark.hadoop.fs.file.impl.disable.crc=true" \
        "$JOBS_DIR/$job_file"

    echo ""
}

if [ $# -eq 0 ]; then
    # Submit both jobs in background, wait together
    submit_job "ward_energy_summary.py" "UrbanPulse Ward Energy Summary" &
    pid1=$!
    submit_job "health_advisories.py" "UrbanPulse Health Advisories" &
    pid2=$!
    wait $pid1 $pid2
    echo "Both jobs submitted."
elif [ "$1" = "ward_energy" ]; then
    submit_job "ward_energy_summary.py" "UrbanPulse Ward Energy Summary"
elif [ "$1" = "health" ]; then
    submit_job "health_advisories.py" "UrbanPulse Health Advisories"
else
    echo "Usage: bash submit_jobs.sh [ward_energy|health]"
    exit 1
fi