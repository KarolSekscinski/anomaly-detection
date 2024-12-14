from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def find_anomalies(
    input_df: DataFrame, price_to_find: bool, threshold: float,
    column_name: str, window_size: int
) -> DataFrame:
    """
    Custom function to find anomalies\n\t
        1. z-score method: if (data points in batch - batch mean) / batch stddev is greater than threshold
        then data point is anomalous
        2. peak and valleys method: if data point is peak / valley in batch then data point is anomalous
        3. isolation forest method: if data point is classified as anomalous by isolation forest
    :param threshold:
    :param price_to_find:
    :param column_name:
    :param window_size:
    :param input_df:
    :return: DataFrame with labeled anomalies
    """
    # UDFs
    # make_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    # source_udf = udf(lambda: str(f"{column_name} anomaly"), StringType())
    window_size_udf = udf(lambda: int(window_size), IntegerType())
    threshold_udf = udf(lambda: float(threshold), DoubleType())
    if price_to_find:
        # MAD (Median Absolute Deviation)
        stats = input_df.selectExpr(
            f"percentile_approx({'price'}, 0.5) as median"
        ).collect()

        median_value = stats[0]["median"]

        mad_df = input_df.withColumn(
            "absolute_deviation", abs(col(column_name) - lit(median_value))
        )

        mad_value = mad_df.selectExpr(
            "percentile_approx(absolute_deviation, 0.5) as mad"
        ).collect()[0]["mad"]

        input_df = input_df \
            .withColumn("z_score", abs(col(column_name) - lit(median_value)) / lit(mad_value)) \
            .withColumn("is_anomaly", (col("z_score") > lit(threshold)).cast(BooleanType())) \
            .filter(col("is_anomaly"))
    else:
        window_spec = Window.orderBy("trade_ts")

        input_df = input_df \
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
            .drop("prev_value", "next_value", "is_peak", "is_valley") \
            .filter(col("is_anomaly"))
            # .withColumn("is_price", when(col("is_volume_anomaly"), True).cast(BooleanType()))

    # Insert appropriate threshold based on source
    output_df = input_df \
        .withColumn("threshold", threshold_udf()) \
        .withColumn("window_size", window_size_udf())

    output_df = output_df.select(
        "uuid", "price", "volume", "ingestion_ts", "symbol", "threshold", "window_size",
        "trade_ts"
    )

    return output_df
