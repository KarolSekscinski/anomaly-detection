from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def run_isolation_forest(features):
    """
    Run the Isolation Forest model from the JAR.
    This could involve invoking the model directly using PySpark's JVM capabilities.
    """
    from pyspark import SparkContext
    from py4j.java_gateway import java_import

    sc = SparkContext.getOrCreate()

    # Import your class from the JAR
    java_import(sc._jvm, "IsolationForestWrapper")

    # Initialize the Isolation Forest class
    isolation_forest = sc._jvm.IsolationForestWrapper()
    result = isolation_forest.detectAnomalies(features)  # Pass the feature vector

    return result




def find_anomalies(
    input_df: DataFrame, type_of_anomaly_to_find: int, threshold: float,
    column_name: str, window_size: int
) -> DataFrame:
    """
    Custom function to find anomalies\n\t
        1. z-score method: if (data points in batch - batch mean) / batch stddev is greater than threshold then data point is anomalous
        2. peak and valleys method: if data point is peak / valley in batch then data point is anomalous
        3. isolation forest method: if data point is classified as anomalous by isolation forest
    :param input_df:
    :param type_of_anomaly_to_find:
    :param threshold:
    :param column_name:
    :param window_size:
    :return:
    """
    # UDFs
    make_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    source_udf = udf(lambda: str(f"{column_name} anomaly"), StringType())
    window_size_udf = udf(lambda: int(window_size), IntegerType())
    threshold_udf = udf(lambda: float(threshold), DoubleType())
    if type_of_anomaly_to_find == 1:
        stats = input_df.select(
            mean(col(column_name)).alias("mean"),
            stddev(col(column_name)).alias("stddev")
        ).collect()

        mean_value = stats[0]["mean"]
        stddev_value = stats[0]["stddev"]

        input_df = input_df \
            .withColumn("uuid", col("uuid")) \
            .withColumn("price", col("price")) \
            .withColumn("volume", col("volume")) \
            .withColumn("z_score", (col(column_name) - mean_value) / stddev_value) \
            .withColumn("is_anomaly", (abs(col("z_score")) > threshold).cast(BooleanType())) \
            .withWatermark("trade_ts", f"{window_size} seconds") \
            .filter(col("is_anomaly") == True)
    elif type_of_anomaly_to_find == 2:
        # Define a window specification for row-based computation
        window_spec = Window.orderBy("trade_ts")

        # Calculate the difference between the current value and its neighbors
        tmp_df = input_df \
            .withColumn("prev_value", lag(col(column_name)).over(window_spec)) \
            .withColumn("next_value", lead(col(column_name)).over(window_spec)) \
            .withColumn("is_peak",
                        ((col(column_name) > col("prev_value")) & (col(column_name) > col("next_value")) &
                         (col(column_name) - least(col("prev_value"), col("next_value")) > threshold)).cast(
                            BooleanType())) \
            .withColumn("is_valley",
                        ((col(column_name) < col("prev_value")) & (col(column_name) < col("next_value")) &
                         (greatest(col("prev_value"), col("next_value")) - col(column_name) > threshold)).cast(
                            BooleanType())) \
            .withColumn("is_anomaly", (col("is_peak") | col("is_valley")).cast(BooleanType())) \
            .filter(col("is_anomaly") == True)

        # Drop intermediate columns if no longer needed
        input_df = input_df.drop("prev_value", "next_value", "is_peak", "is_valley")

        input_df = tmp_df \
            .withColumn("uuid", col("uuid")) \
            .withColumn("price", col("price")) \
            .withColumn("volume", col("volume")) \
            .withWatermark("trade_ts", f"{window_size} seconds") \

    elif type_of_anomaly_to_find == 3:
        # Define UDF for Isolation Forest
        isolation_forest_udf = udf(
            lambda features: run_isolation_forest(features),
            ArrayType(FloatType())  # Adjust return type based on your implementation
        )

        input_df = input_df.withColumn("isolation_forest_scores",
                                       isolation_forest_udf(col(column_name))) \
            .withColumn("is_anomaly", col("isolation_forest_scores")[1] > threshold) \
            .filter(col("is_anomaly") == True)


    output_df = input_df \
        .withColumn("anomaly_uuid", make_uuid()) \
        .withColumn("ingestion_ts", current_timestamp()) \
        .withColumn("anomaly_source", source_udf()) \
        .withColumn("window_size", window_size_udf()) \
        .withColumn("threshold", threshold_udf())

    # Select only the columns needed for the anomalies table
    anomalies_df = output_df.select(
        "uuid", "price", "volume", "anomaly_source", "anomaly_uuid",
        "window_size", "threshold", "trade_ts", "ingestion_ts"
    )

    return anomalies_df
