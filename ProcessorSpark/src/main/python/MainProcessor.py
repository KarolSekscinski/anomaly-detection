from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import current_timestamp
from Settings import Settings
from anomalies_functions import find_anomalies
import uuid
from pyspark.sql.functions import udf


class MainProcessor:
    @staticmethod
    def main():
        def process_batch(batch_df, _, func_name, **kwargs):
            """
            Process each batch using the specified anomaly detection function.

            :param batch_df: Spark DataFrame for the batch
            :param _: batch_id
            :param func_name: Function to use for anomaly detection
            :param kwargs: Additional keyword arguments for the anomaly detection function
            """
            # Apply the specified anomaly detection function
            anomalies_df = func_name(batch_df, **kwargs)
            # Write detected anomalies to Cassandra
            anomalies_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(
                    table=settings_for_spark.cassandra["tables"]["anomalies"],
                    keyspace=settings_for_spark.cassandra["keyspace"]
                ) \
                .mode("append") \
                .save()

        # This line reads configuration class
        settings_for_spark = Settings()

        # This line creates dict for anomalies settings, it contains list of window_sizes, thresholds and
        # contamination_factors
        setting_for_anomalies = settings_for_spark.anomalies
        print(setting_for_anomalies)

        # This line creates spark session
        spark = SparkSession.builder \
            .appName(settings_for_spark.spark["appName"][0]["StreamProcessor"]) \
            .config("spark.cassandra.connection.host", settings_for_spark.cassandra["host"]) \
            .config("spark.cassandra.connection.port", settings_for_spark.cassandra["port"]) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                                           "org.apache.spark:spark-avro_2.12:3.2.0,"
                                           "com.datastax.oss:java-driver-core:4.13.0") \
            .getOrCreate()
        print(dir(spark))

        # Read Avro schema and Kafka stream
        json_format_schema = open(settings_for_spark.schemas["trades"], "r").read()

        input_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings_for_spark.kafka["server_address"]) \
            .option("subscribe", settings_for_spark.kafka["topic"][0]["market"]) \
            .option("minPartitions", settings_for_spark.kafka["min_partitions"][0]["StreamProcessor"]) \
            .option("maxOffsetsPerTrigger", settings_for_spark.spark["max_offsets_per_trigger"][0]["StreamProcessor"]) \
            .option("useDeprecatedOffsetFetching", settings_for_spark.spark["deprecated_offsets"][0]["StreamProcessor"]) \
            .load()

        expanded_df = input_df \
            .withColumn("avroData", from_avro(col("value"), json_format_schema).alias("trades")) \
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
            .withColumn("ingestion_ts", current_timestamp())

        query = final_df \
            .writeStream \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(lambda batch_df, batch_id:
                          batch_df.write
                          .format("org.apache.spark.sql.cassandra")
                          .options(table=settings_for_spark.cassandra["tables"]["trades"],
                                   keyspace=settings_for_spark.cassandra["keyspace"])
                          .mode("append")
                          .save()
                          ) \
            .outputMode("update") \
            .start()

        for window_size in setting_for_anomalies["window_sizes"]["prices"]:
            anomalies_query_simple = final_df \
                .writeStream \
                .trigger(processingTime=f"{window_size} seconds") \
                .foreachBatch(
                    lambda batch_df, batch_id: process_batch(
                        batch_df, batch_id, func_name=find_anomalies,
                        price_to_find=True,
                        threshold=setting_for_anomalies["thresholds"]["z-threshold"], column_name="price",
                        window_size=window_size
                    )
                ) \
                .outputMode("update") \
                .start()

        for window_size in setting_for_anomalies["window_sizes"]["volumes"]:
            anomalies_query_p_and_v = final_df \
                .writeStream \
                .trigger(processingTime=f"{window_size} seconds") \
                .foreachBatch(
                    lambda batch_df, batch_id: process_batch(
                        batch_df, batch_id, func_name=find_anomalies,
                        price_to_find=False,
                        threshold=setting_for_anomalies["thresholds"]["pv-threshold"], column_name="volume",
                        window_size=window_size
                    )
                ) \
                .outputMode("update") \
                .start()

        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    MainProcessor().main()
