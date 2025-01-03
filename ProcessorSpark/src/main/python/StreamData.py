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
        def write_to_cassandra(batch_df, batch_id, cassandra_settings, table_name):
            row_size = batch_df.count()
            if row_size > 0:
                # Add the batch_size column
                batch_df = batch_df.withColumn("batch_size", lit(row_size))
                batch_df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(
                        table=table_name,
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


        enriched_df = (final_df
                       .withColumn("price_z_score", arima_anomalies(col("price")))
                       .withColumn("volume_z_score", arima_anomalies(col("volume")))
                       ).repartition(100)

        base_data = enriched_df.select("uuid", "symbol", "price", "volume", "trade_ts", "ingestion_ts")

        # Write base data to Cassandra
        unified_query = base_data.writeStream \
            .foreachBatch(lambda batch_df, batch_id:
                          write_to_cassandra(batch_df, batch_id, settings.cassandra,
                                             settings.cassandra["tables"]["trades"])) \
            .outputMode("append") \
            .start()

        price_anomalies = enriched_df \
            .filter(col("price_z_score") == True) \
            .select("uuid", "symbol", "price", "trade_ts", "ingestion_ts")

        price_query = price_anomalies.writeStream \
            .foreachBatch(lambda batch_df, batch_id:
                          write_to_cassandra(batch_df, batch_id, settings.cassandra,
                                             settings.cassandra["tables"]["price"])) \
            .outputMode("append") \
            .start()

        volume_anomalies = enriched_df \
            .filter(col("volume_z_score") == True) \
            .select("uuid", "symbol", "volume", "trade_ts", "ingestion_ts")

        volume_query = volume_anomalies.writeStream \
            .foreachBatch(lambda batch_df, batch_id:
                          write_to_cassandra(batch_df, batch_id, settings.cassandra,
                                             settings.cassandra["tables"]["volume"])) \
            .outputMode("append") \
            .start()

        spark_context.streams.awaitAnyTermination()


if __name__ == "__main__":
    StreamData().main()
