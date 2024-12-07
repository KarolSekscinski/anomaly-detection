from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid


def find_anomalies_using_z_threshold_rule(input_df, threshold: float, column_name: str, window_size: int):
    """

    :param input_df:
    :param threshold:
    :param column_name:
    :param window_size:
    :return:
    """
    # UDFs
    make_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    z_score_udf = udf(lambda: str("z-score anomaly"), StringType())
    window_size_udf = udf(lambda: int(window_size), IntegerType())
    threshold_udf = udf(lambda: float(threshold), DoubleType())

    # Calculate mean and standard deviation for the batch
    stats = input_df.select(
        mean(col(column_name)).alias("mean"),
        stddev(col(column_name)).alias("stddev")
    ).collect()

    mean_value = stats[0]["mean"]
    stddev_value = stats[0]["stddev"]

    tmp_df = input_df \
        .withColumn("price", col("price")) \
        .withColumn("volume", col("volume")) \
        .withColumn("z_score", (col(column_name) - mean_value) / stddev_value) \
        .withColumn("is_anomaly", (abs(col("z_score")) > threshold).cast(BooleanType())) \
        .withWatermark("trade_ts", f"{window_size} seconds") \
        .filter(col("is_anomaly") == True)

    output_df = tmp_df \
        .withColumn("uuid", make_uuid()) \
        .withColumn("ingestion_ts", current_timestamp()) \
        .withColumn("anomaly_source", z_score_udf()) \
        .withColumn("window_size", window_size_udf()) \
        .withColumn("threshold", threshold_udf())

    # Select only the columns needed for the anomalies table
    anomalies_df = output_df.select(
        "uuid", "price", "volume", "anomaly_source",
        "window_size", "threshold", "trade_ts", "ingestion_ts"
    )

    return anomalies_df


def find_anomalies_using_isolation_forest(data, contamination: float):
    pass


def find_anomalies_using_one_class_svm(data, contamination: float):
    pass
