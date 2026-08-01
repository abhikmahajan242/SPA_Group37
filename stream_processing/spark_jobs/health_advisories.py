from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col


aqi_schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("zone", StringType()) \
    .add("pm25", IntegerType()) \
    .add("pm10", IntegerType()) \
    .add("no2", IntegerType()) \
    .add("aqi", IntegerType()) \
    .add("timestamp", TimestampType())


def main():
    spark = SparkSession.builder \
        .appName("UrbanPulse Health Advisories") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # zone_profile is a small, static lookup table used to enrich each advisory.
    zone_profile = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/opt/spark/data/zone_profile.csv")

    zone_profile.createOrReplaceTempView("zone_profile")

    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "urbanpulse.air_quality") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), aqi_schema).alias("data")) \
        .select("data.*")

    # Bound state retained for overlapping event-time windows while allowing
    # AQI records to arrive up to five minutes late.
    parsed_with_watermark = parsed \
        .withWatermark("timestamp", "5 minutes")

    parsed_with_watermark.createOrReplaceTempView("aqi_stream")

    # A 10-minute window sliding every five minutes produces a rolling AQI
    # average, enriches it from the static profile, then keeps unhealthy zones.
    result = spark.sql("""
        SELECT
            a.zone,
            window.end AS window_end,
            AVG(a.aqi) AS rolling_avg_aqi,
            z.population,
            z.num_schools
        FROM aqi_stream a
        JOIN zone_profile z ON a.zone = z.zone
        GROUP BY a.zone, window(a.timestamp, '10 minutes', '5 minutes'), z.population, z.num_schools
        HAVING AVG(a.aqi) > 150
    """)

    # Kafka sinks require key/value columns; zone keeps related advisories together.
    kafka_output = result.selectExpr("zone AS key", "to_json(struct(*)) AS value")

    query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "urbanpulse.health_advisories") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/health_advisories") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
