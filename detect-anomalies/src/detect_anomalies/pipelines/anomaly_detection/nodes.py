from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
import numpy as np
from tensorflow import keras


def consume_kafka_data(topic_name: str, bootstrap_servers_config: dict) -> pd.DataFrame:
    """
    Consumes data from a Kafka topic and returns it as a Pandas DataFrame.

    Args:
        topic_name (str): The name of the Kafka topic to consume from.
        bootstrap_servers_config (dict): A dictionary containing Kafka bootstrap server configuration
            (e.g., {'host': 'kafka-broker', 'port': 29092}).

    Returns:
        pd.DataFrame: A DataFrame containing the consumed data.
    """
    bootstrap_servers = f"{bootstrap_servers_config['host']}:{bootstrap_servers_config['port']}"
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
    """
    Preprocesses the raw Kafka data by filling NaN values.

    Args:
        raw_data (pd.DataFrame): The raw data received from Kafka.

    Returns:
        pd.DataFrame: The preprocessed data with NaN values replaced by 0.
    """
    raw_data.fillna(0, inplace=True)  # Replace NaNs with 0
    return raw_data


def detect_anomalies_using_high_volume(data: pd.DataFrame, high_volume_threshold: float) -> pd.DataFrame:
    """
    Detects anomalies in data based on a high volume threshold.

    Args:
        data (pd.DataFrame): The input data containing a 'size' column.
        high_volume_threshold (float): The threshold above which a data point is considered an anomaly.

    Returns:
        pd.DataFrame: A DataFrame containing the detected high-volume anomalies.
    """
    data['high_volume_anomaly'] = data['size'] > high_volume_threshold
    anomalies = data[data['high_volume_anomaly']]
    return anomalies


def detect_anomalies_using_low_volume(data: pd.DataFrame, low_volume_threshold: float) -> pd.DataFrame:
    """
    Detects anomalies in data based on a low volume threshold.

    Args:
        data (pd.DataFrame): The input data containing a 'volume' column.
        low_volume_threshold (float): The threshold below which a data point is considered an anomaly.

    Returns:
        pd.DataFrame: A DataFrame containing the detected low-volume anomalies.
    """
    data['low_volume_anomaly'] = data['volume'] < low_volume_threshold
    anomalies = data[data['low_volume_anomaly']]
    return anomalies


def detect_anomalies_using_isolation_forest(data: pd.DataFrame, isolation_forest_config: dict) -> pd.DataFrame:
    """
    Detects anomalies using the Isolation Forest algorithm.

    Args:
        data (pd.DataFrame): The input data with numeric columns for model training.
        isolation_forest_config (dict): Configuration dictionary for the Isolation Forest model
            (e.g., {'n_estimators': 100, 'contamination': 0.1 }).

    Returns:
        pd.DataFrame: A DataFrame containing the detected anomalies with an 'anomaly_isolation_forest' column.
    """
    model = IsolationForest(**isolation_forest_config)
    features = data.select_dtypes(include=[np.number])  # Use only numeric columns for anomaly detection
    data['anomaly_isolation_forest'] = model.fit_predict(features)
    anomalies = data[data['anomaly_isolation_forest'] == -1]  # -1 indicates an anomaly
    return anomalies


def detect_anomalies_using_autoencoder(data: pd.DataFrame, autoencoder_config: dict) -> pd.DataFrame:
    """
    Detects anomalies using an autoencoder model.

    Args:
        data (pd.DataFrame): The input data with numeric columns for training the autoencoder.
        autoencoder_config (dict): Configuration dictionary for the autoencoder model
            (e.g., {'encoding_dim': 16, 'epochs': 50, 'batch_size': 32, 'mse_threshold': 0.05}).

    Returns:
        pd.DataFrame: A DataFrame containing the detected anomalies with an 'anomaly_autoencoder' column.
    """
    features = data.select_dtypes(include=[np.number])
    input_dim = features.shape[1]

    # Build a simple autoencoder model
    model = keras.Sequential([
        keras.layers.InputLayer(input_shape=(input_dim,)),
        keras.layers.Dense(autoencoder_config['encoding_dim'], activation='relu'),
        keras.layers.Dense(input_dim, activation='sigmoid')
    ])

    model.compile(optimizer='adam', loss='mse')

    # Train the model
    model.fit(features, features, epochs=autoencoder_config['epochs'], batch_size=autoencoder_config['batch_size'],
              verbose=0)

    # Calculate reconstruction error
    reconstruction = model.predict(features)
    mse = np.mean(np.power(features - reconstruction, 2), axis=1)

    # Mark anomalies based on a threshold
    data['anomaly_autoencoder'] = mse > autoencoder_config['mse_threshold']
    anomalies = data[data['anomaly_autoencoder']]
    return anomalies


def detect_anomalies_using_one_class_svm(data: pd.DataFrame, svm_config: dict) -> pd.DataFrame:
    """
    Detects anomalies using the One-Class SVM algorithm.

    Args:
        data (pd.DataFrame): The input data with numeric columns for model training.
        svm_config (dict): Configuration dictionary for the One-Class SVM model
            (e.g., {'kernel': 'rbf', 'gamma': 'auto'}).

    Returns:
        pd.DataFrame: A DataFrame containing the detected anomalies with an 'anomaly_one_class_svm' column.
    """
    model = OneClassSVM(**svm_config)
    features = data.select_dtypes(include=[np.number])
    data['anomaly_one_class_svm'] = model.fit_predict(features)
    anomalies = data[data['anomaly_one_class_svm'] == -1]  # -1 indicates an anomaly
    return anomalies


def produce_kafka_data(data: pd.DataFrame, topic_name, bootstrap_servers_config: dict) -> None:
    """
    Produces data to a Kafka topic.

    Args:
        data (pd.DataFrame): The data to be produced to Kafka.
        topic_name (str): The name of the Kafka topic to send data to.
        bootstrap_servers_config (dict): A dictionary containing Kafka bootstrap server configuration
            (e.g., {'host': 'kafka-broker', 'port': 29092}).

    Returns:
        None
    """
    bootstrap_servers = f"{bootstrap_servers_config['host']}:{bootstrap_servers_config['port']}"
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for _, row in data.iterrows():
        producer.send(topic_name, row.to_dict())

    producer.flush()
    producer.close()
    print(f"Anomalies sent to Kafka topic: {topic_name}")
