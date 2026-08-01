from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, window, sum, avg, max, to_date


smart_meter_schema = StructType() \
    .add("meter_id", StringType()) \
    .add("ward_id", StringType()) \
    .add("kwh_reading", DoubleType()) \
    .add("voltage", DoubleType()) \
    .add("power_factor", DoubleType()) \
    .add("timestamp", TimestampType())


def main():
    spark = SparkSession.builder \
        .appName("UrbanPulse Ward Energy Summary") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "urbanpulse.smart_meters") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), smart_meter_schema).alias("data")) \
        .select("data.*")

    windowed = parsed \
        .withWatermark("timestamp", "45 minutes") \
        .groupBy(col("ward_id"), window(col("timestamp"), "15 minutes")) \
        .agg(
            sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            max("voltage").alias("peak_voltage")
        )

    # Kafka sink — update mode emits evolving window results for live dashboards
    kafka_query = windowed.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "ward_energy_summary") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/ward_energy_kafka") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()

    # Parquet sink — append mode writes each finalized window exactly once
    parquet_query = windowed \
        .withColumn("date", to_date(col("window.start"))) \
        .writeStream \
        .format("parquet") \
        .partitionBy("ward_id", "date") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/ward_energy_parquet") \
        .option("path", "/opt/spark/data/parquet/ward_energy") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

    kafka_query.awaitTermination()
    parquet_query.awaitTermination()


if __name__ == "__main__":
    main()
