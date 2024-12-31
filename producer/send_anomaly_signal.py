import time
import pandas as pd
import json
from kafka import KafkaProducer
import avro.schema
import avro.io
import logging
from datetime import datetime
import io


def load_config(config_file):
    # This function loads config file for Producer class. The config should include
    # e.g. Finnhub API Key
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config


def load_kafka_producer(kafka_server):
    # This function returns Kafka Producer to later produce data from
    # Finnhub.io into Kafka
    return KafkaProducer(bootstrap_servers=kafka_server)


def load_avro_schema(path_to_avro_schema):
    # This function parses avro schema based on response from
    # sample response https://finnhub.io/docs/api/websocket-trades
    return avro.schema.parse(open(path_to_avro_schema).read())


def encode_avro_message(data, schema):
    # This function produces an Avro-encoded binary message based on schema
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    datum_writer = avro.io.DatumWriter(schema)

    datum_writer.write(data, encoder)
    return bytes_writer.getvalue()


class Producer:
    def __init__(self):
        self.config = load_config('src/config.json')
        self.kafka_producer = load_kafka_producer(self.config['KAFKA_SERVER'])
        self.avro_schema = load_avro_schema('src/schemas/trades.avsc')

    def run_server(self):
        logging.info("Server started")
        self.send_to_kafka("../sandbox/data/finnhub1h.csv")

    def send_to_kafka(self, filename):
        df = pd.read_csv(filename)
        topic_name = self.config["KAFKA_TOPIC"]
        for _, row in df.iterrows():
            time.sleep(0.5)
            trade = row.to_dict()
            trade['trade_ts'] = str(datetime.now())
            avro_message = encode_avro_message(trade, self.avro_schema)
            self.kafka_producer.send(topic_name, value=avro_message)
        self.kafka_producer.flush()
        print(f"All rows from {filename} have been sent to {topic_name}")


if __name__ == "__main__":
    producer = Producer()
    producer.run_server()