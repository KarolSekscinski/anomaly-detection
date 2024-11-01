from kedro.pipeline import Pipeline, node
from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=consume_kafka_data,
            inputs={
                "topic_name": "params:kafka_topics.input",
                "bootstrap_servers_config": "params:kafka_server",
            },
            outputs="raw_kafka_data",
            name="consume_kafka_node",
        ),
        node(
            func=preprocess_raw_kafka_data,
            inputs={
                "raw_data": "raw_kafka_data"
            },  # Here insert preprocessing rules
            outputs="processed_kafka_data",
            name="preprocess_raw_kafka_data_node",

        ),
        node(
            func=detect_anomalies_using_high_volume,
            inputs={
                "data": "processed_kafka_data",
                "high_volume_threshold": "params:thresholds.high_volume"},
            outputs="anomalies_using_high_volume_rule",
            name="detect_anomalies_using_high_volume_rule_node"
        ),
        node(
            func=detect_anomalies_using_low_volume,
            inputs={"data": "processed_kafka_data", "low_volume_threshold": "params:thresholds.low_volume"},
            outputs="anomalies_using_low_volume_rule",
            name="detect_anomalies_using_low_volume_rule_node"
        ),
        node(
            func=detect_anomalies_using_isolation_forest,
            inputs={"data": "processed_kafka_data", "isolation_forest_config": "params:isolation_forest"},
            outputs="anomalies_using_isolation_forest_rule",
            name="anomalies_using_isolation_forest_rule_node"
        ),
        # TODO: verify if needed
        # node(
        #     func=detect_anomalies_using_autoencoder,
        #     inputs={"data": "processed_kafka_data", "autoencoder_config": "params:autoencoder"},
        #     outputs="anomalies_using_decoder_rule",
        #     name="anomalies_using_decoder_rule_node"
        # ),
        # node(
        #     func=detect_anomalies_using_one_class_svm,
        #     inputs={"data": "processed_kafka_data", "one_class_svm_config": "params:"},
        #     outputs="anomalies_using_one_class_svm_rule",
        #     name="anomalies_using_one_class_svm_rule_node"
        # ),
        node(
            func=produce_kafka_data,
            inputs={
                "topic_name": "params:kafka_topics.output",
                "bootstrap_servers_config": "params:kafka_server",
                "anomalies_high_volume": "anomalies_using_high_volume_rule",
                "anomalies_low_volume": "anomalies_using_low_volume_rule",
                "anomalies_using_isolation_forest": "anomalies_using_isolation_forest_rule"
                    },
            outputs=None,
            name="produce_kafka_node",
        )
    ])
