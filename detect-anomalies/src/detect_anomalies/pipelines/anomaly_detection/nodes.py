from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
from sklearn.ensemble import IsolationForest


def consume_kafka_data(topic_name, bootstrap_servers) -> pd.DataFrame:
    """Consumes data from a Kafka topic."""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    data = []
    for message in consumer:
        data.append(message.value)
        # Stop after a certain number of messages for batch processing (adjust as needed)
        if len(data) >= 1000:  # Example condition for testing
            break

    consumer.close()
    return pd.DataFrame(data)


def preprocess_raw_kafka_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the raw Kafka data"""
    return raw_data


def detect_anomalies_using_high_volume(data: pd.DataFrame, high_volume_threshold) -> pd.DataFrame:
    pass


def detect_anomalies_using_low_volume(low_volume_threshold):
    pass


def detect_anomalies_using_isolation_forest():
    pass


# def detect_anomalies_using_decoder():
#     pass
#
#
# def detect_anomalies_using_oneclass_svm():
#     pass


def detect_anomalies(data: pd.DataFrame) -> pd.DataFrame:
    """Applies the IsolationForest algorithm to detect anomalies."""
    model = IsolationForest(contamination=0.01)
    data['anomaly'] = model.fit_predict(data)

    # Extract anomalies (anomalies are labeled as -1)
    anomalies = data[data['anomaly'] == -1]
    print(f"Number of anomalies detected: {len(anomalies)}")

    return anomalies


def produce_kafka_data(data: pd.DataFrame, topic_name, bootstrap_servers='localhost:9092'):
    """Produces data to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for _, row in data.iterrows():
        producer.send(topic_name, row.to_dict())

    producer.flush()
    producer.close()
    print(f"Anomalies sent to Kafka topic: {topic_name}")


def load_config(config_path):
    with open(config_path, "r") as file:
        config = json.load(file)
    return config