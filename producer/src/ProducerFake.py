import logging
import time
import numpy as np
from tqdm import tqdm
import pandas as pd
from utils.producer_functions import *


class ProducerFake:
    def __init__(self):
        self.config = load_config('config_fake.json')
        self.kafka_producer = load_kafka_producer(self.config['KAFKA_SERVER'])
        self.avro_schema = load_avro_schema('schemas/trades.avsc')

    def run_server(self):
        print("Waiting 3 min for other services to start")
        time.sleep(3 * 60)
        print("Server started")
        self.find_files_and_send_to_kafka()

    def find_files_and_send_to_kafka(self):
        files = self.config['DATASET_PATH']
        print(files)
        for file_path in tqdm(files):
            self.send_csv_files_to_kafka(file_path)

    def send_csv_files_to_kafka(self, csv_file: str):
        df = pd.read_csv(csv_file)
        df = df.drop(["trade_ts", "price_anomaly", "volume_anomaly", "trade_conditions"], axis=1, errors='ignore')
        df = df.rename(columns=self.config["COLUMN_NAMES"])
        for i, tuple_row in enumerate(df.iterrows()):
            trade = tuple_row[1].to_dict()
            if i % 15 == 0:
                time.sleep(np.random.uniform(0.6, 1.2))
            # Create the Kafka message structure
            data = format_trade_message(trade)
            print(f"Sending data: {data}")
            avro_message = encode_avro_message(data, self.avro_schema)

            self.kafka_producer.send(self.config['KAFKA_TOPIC'], avro_message)
        self.kafka_producer.flush()
        print(f"All rows from {csv_file} have been sent to {self.config['KAFKA_TOPIC']}")

print("end of file")
if __name__ == "__main__":
    print("Starting producer")
    producer = ProducerFake()
    producer.run_server()
