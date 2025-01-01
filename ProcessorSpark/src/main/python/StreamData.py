from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *

import uuid
from Settings import Settings
from detection_methods import *
import pandas as pd


class StreamData:
    @staticmethod
    def main():
        # Write to Cassandra functions
        def write_to_cassandra(batch_df, batch_id, cassandra_settings):
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(
                    table=cassandra_settings["tables"]["trades"],
                    keyspace=cassandra_settings["keyspace"]
                ) \
                .mode("append") \
                .save()

        def write_anomalies(batch_df, batch_id, cassandra_settings, anomaly_type):
            table = cassandra_settings["tables"][anomaly_type]
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(
                    table=table,
                    keyspace=cassandra_settings["keyspace"]
                ) \
                .mode("append") \
                .save()

        # Main function
        settings = Settings()
        spark_context = SparkSession.builder \
            .appName(settings.spark["appName"][0]["StreamProcessor"]) \
            .config("spark.cassandra.connection.host", settings.cassandra["host"]) \
            .config("spark.cassandra.connection.port", settings.cassandra["port"]) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                                           "org.apache.spark:spark-avro_2.12:3.5.0"
                                           "com.datastax.oss:java-driver-core:4.13.0") \
            .getOrCreate()

        json_schema = open(settings.schemas["trades"], "r").read()

        input_df = spark_context \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.kafka["server_address"]) \
            .option("subscribe", settings.kafka["topic"][0]["market"]) \
            .option("minPartitions", settings.kafka["min_partitions"][0]["StreamProcessor"]) \
            .option("maxOffsetsPerTrigger", settings.spark["max_offsets_per_trigger"][0]["StreamProcessor"]) \
            .load()

        expanded_df = input_df \
            .withColumn("avroData", from_avro(col("value"), json_schema).alias("trades")) \
            .selectExpr("avroData.*")
        expanded_df.printSchema()

        exploded_df = expanded_df \
            .withColumn("trade", explode(col("data"))) \
            .select("trade.*", "type")

        # Define UDFs
        make_uuid = udf(lambda: str(uuid.uuid4()), StringType())

        # Rename columns to match their names in Cassandra db
        final_df = exploded_df \
            .withColumn("uuid", make_uuid()) \
            .withColumnRenamed("c", "trade_conditions") \
            .withColumnRenamed("p", "price") \
            .withColumnRenamed("s", "symbol") \
            .withColumnRenamed("t", "trade_ts") \
            .withColumnRenamed("v", "volume") \
            .withColumn("trade_ts", (col("trade_ts") / 1000).cast("timestamp")) \
            .withColumn("ingestion_ts", lit(current_timestamp()))

        # Add anomaly detection columns directly in the main stream
        enriched_df = (final_df
                       .withColumn("price_z_score", z_score_anomalies(col("price")))
                       # .withColumn("price_iqr_score", iqr_anomalies(col("price")))
                       # .withColumn("price_arima_score", arima_anomalies(col("price")))
                       # .withColumn("price_iforest_score", iforest_anomalies(col("price")))
                       .withColumn("price_is_anomaly", col("price_z_score"))
                       .withColumn("volume_z_score", z_score_anomalies(col("volume")))
                       # .withColumn("volume_iqr_score", iqr_anomalies(col("volume")))
                       # .withColumn("volume_iforest_score", iforest_anomalies(col("volume")))
                       .withColumn("volume_is_anomaly", col("volume_z_score"))
                       .withColumn("detection_ts", current_timestamp())
                       )

        # Split data for base and anomaly-specific processing
        base_data = enriched_df.select("uuid", "symbol", "price", "volume", "trade_ts", "ingestion_ts")

        price_anomalies = enriched_df \
            .filter(col("price_is_anomaly")) \
            .select("uuid", "symbol", "price", "trade_ts", "ingestion_ts", "detection_ts")

        volume_anomalies = enriched_df \
            .filter(col("volume_is_anomaly")) \
            .select("uuid", "symbol", "volume", "trade_ts", "ingestion_ts", "detection_ts")

        # Write base data to Cassandra
        unified_query = base_data.writeStream \
            .trigger(processingTime="6 seconds") \
            .foreachBatch(lambda batch_df, batch_id: write_to_cassandra(batch_df, batch_id, settings.cassandra)) \
            .outputMode("update") \
            .start()

        # Write price anomalies to Cassandra
        price_anomalies.writeStream \
            .trigger(processingTime="6 seconds") \
            .foreachBatch(lambda batch_df, batch_id: write_anomalies(batch_df, batch_id, settings.cassandra, "price")) \
            .outputMode("update") \
            .start()

        # Write volume anomalies to Cassandra
        volume_anomalies.writeStream \
            .trigger(processingTime="6 seconds") \
            .foreachBatch(lambda batch_df, batch_id: write_anomalies(batch_df, batch_id, settings.cassandra, "volume")) \
            .outputMode("update") \
            .start()

        spark_context.streams.awaitAnyTermination()


if __name__ == "__main__":
    StreamData().main()
