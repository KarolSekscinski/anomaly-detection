from kedro.pipeline import Pipeline, node
from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=consume_kafka_data,
            inputs=None,
            outputs="raw_kafka_data",
            name="consume_kafka_node",
            kwargs={
                "topic_name": "params:kafka-topics.input",
                "bootstrap_servers_config": "params:kafka-server",
            }
        ),
        node(
            func=preprocess_raw_kafka_data,
            inputs="raw_kafka_data",
            outputs="processed_kafka_data",
            name="preprocess_raw_kafka_data_node",
            kwargs={} # tutaj sterowanie preprocessingiem danych np. co nalezy zrobic
        ),
        node(
            func=detect_anomalies_using_high_volume,
            inputs="processed_kafka_data",
            outputs="anomalies_using_high_volume_rule",
            name="detect_anomalies_using_high_volume_rule_node",
            kwargs={"high_volume_threshold": "params:thresholds.high-volume"}
        ),
        node(
            func=detect_anomalies_using_low_volume,
            inputs="processed_kafka_data",
            outputs="anomalies_using_low_volume_rule",
            name="detect_anomalies_using_low_volume_rule_node",
            kwargs={"low_volume_threshold": "params:thresholds.low-volume"}
        ),
        node(
            func=detect_anomalies_using_isolation_forest,
            inputs="processed_kafka_data",
            outputs="anomalies_using_isolation_forest_rule",
            name="anomalies_using_isolation_forest_rule_node",
            kwargs={"isolation_forest_config": "params:isolation_forest"}
        ),
        node(
            func=detect_anomalies_using_autoencoder,
            inputs="processed_kafka_data",
            outputs="anomalies_using_decoder_rule",
            name="anomalies_using_decoder_rule_node",
            kwargs={"autoencoder": "params:autoencoder"}
        ),
        node(
            func=detect_anomalies_using_one_class_svm,
            inputs="processed_kafka_data",
            outputs="anomalies_using_one_class_svm_rule",
            name="anomalies_using_one_class_svm_rule_node",
            kwargs={"svm": "params:svm"}
        ),
        node(
            func=produce_kafka_data,
            inputs=["anomalies_using_high_volume_rule",
                    "anomalies_using_low_volume_rule",
                    "anomalies_using_isolation_forest_rule",
                    "anomalies_using_autoencoder_rule",
                    "anomalies_using_one_class_svm_rule"
                    ],
            outputs=None,
            name="produce_kafka_node",
            kwargs={
                "topic_name": "params:kafka-topics.output",
                "bootstrap_servers_config": "params:kafka-server",
            }
        )
    ])
