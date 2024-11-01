from utils.producer_functions import *
import logging
from tqdm import tqdm
import pandas as pd
from datetime import datetime


class Producer:
    def __init__(self):
        self.config = load_config('config.json')
        self.kafka_config = self.config["KAFKA_SERVER"]
        self.kafka_producer = load_kafka_producer(self.kafka_config["HOST"],
                                                  self.kafka_config["PORT"])

    def run_server(self):
        logging.info("Server started")
        self.find_files_and_send_to_kafka()

    def find_files_and_send_to_kafka(self):
        files = find_files_matching_glob_pattern(self.config['DATASET_PATHS']['GOOGLE'])
        for file_path in tqdm(files):
            self.send_csv_files_to_kafka(file_path, self.config["TOPIC_NAME"])

    def send_csv_files_to_kafka(self, csv_file: str, topic_name: str):
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            trade = row.to_dict()
            trade['producing_ts'] = str(datetime.now())
            json_data = json.dumps(trade)
            self.kafka_producer.send(topic_name, value=json_data)

        self.kafka_producer.flush()
        print(f"All rows from {csv_file} have been sent to {topic_name}")


if __name__ == "__main__":
    producer = Producer()
    producer.run_server()
