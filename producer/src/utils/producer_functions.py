import json
from glob import glob
from kafka import KafkaProducer


def load_config(config_file_path):
    with open(config_file_path, "r") as file:
        config = json.load(file)
    return config


def load_kafka_producer(kafka_host, kafka_port):
    kafka_server = f"{kafka_host}:{kafka_port}"
    return KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def find_files_matching_glob_pattern(pattern) -> []:
    files = glob(pattern)
    files.sort()
    return files


