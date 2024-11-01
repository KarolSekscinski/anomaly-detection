from kedro.pipeline import Pipeline, node
from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    config = load_config(config_path="config.json")
    bootstrap_servers = f"{config['KAFKA_SERVER']['HOST']}:{config['KAFKA_SERVER']['PORT']}"
    high_volume_threshold, low_volume_threshold = (config["THRESHOLDS"]["HIGH_VOLUME"],
                                                   config["THRESHOLDS"]["LOW_VOLUME"])
    return Pipeline([
        node(
            func=consume_kafka_data,
            inputs=None,
            outputs="raw_kafka_data",
            name="consume_kafka_node",
            kwargs={
                "topic_name": config["KAFKA_TOPIC"]["INPUT"],
                "bootstrap_servers": bootstrap_servers
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
            kwargs={"high_volume_threshold": high_volume_threshold}
        ),
        node(
            func=detect_anomalies_using_low_volume,
            inputs="processed_kafka_data",
            outputs="anomalies_using_low_volume_rule",
            name="detect_anomalies_using_low_volume_rule_node",
            kwargs={"low_volume_threshold": low_volume_threshold}
        ),
        node(
            func=detect_anomalies_using_isolation_forest,
            inputs="processed_kafka_data",
            outputs="anomalies_using_isolation_forest_rule",
            name="anomalies_using_isolation_forest_rule_node"
        ),
        # node(
        #     func=detect_anomalies_using_decoder,
        #     inputs="processed_kafka_data",
        #     outputs="anomalies_using_decoder_rule",
        #     name="anomalies_using_decoder_rule_node"
        # ),
        # node(
        #     func=detect_anomalies_using_oneclass_svm,
        #     inputs="processed_kafka_data",
        #     outputs="anomalies_using_oneclass_svm_rule",
        #     name="anomalies_using_oneclass_svm_rule_node"
        # ),
        node(
            func=produce_kafka_data,
            inputs=["anomalies_using_isolation_forest_rule"],
            outputs=None,
            name="produce_kafka_node",
            kwargs={"topic_name": "anomalies", "bootstrap_servers": "localhost:9092"}
        )
    ])
