from websocket_server import WebsocketServer
from producer_functions import *
import logging
from tqdm import tqdm
import pandas as pd


class Producer:
    def __init__(self):
        self.config = load_config('config.json')
        self.kafka_config = self.config["KAFKA_SERVER"]
        self.kafka_producer = load_kafka_producer(self.kafka_config["HOST"],
                                                  self.kafka_config["PORT"])

    def run_server(self):
        server = WebsocketServer(host=self.config['WEBSOCKET_SERVER']['host'],
                                 port=self.config['WEBSOCKET_SERVER']['port'])
        server.set_fn_new_client(self.new_client)
        server.set_fn_message_received(self.message_received)
        logging.info("Server started")
        server.run_forever()

    def new_client(self, client, server):
        logging.info(f"New client connected: {client['id']}")
        server.send_message(client, "Welcome to the WebSocket Server!")

    def message_received(self, client, server, message):
        logging.info(f"Message received: {message}")
        if message == "start_sending":
            self.find_files_and_send_to_kafka()
            server.send_message(client, "Data sent to Kafka!")

    def find_files_and_send_to_kafka(self):
        files = find_files_matching_glob_pattern(self.config['DATASET_PATHS']['GOOGLE'])
        for file_path in tqdm(files):
            self.send_csv_files_to_kafka(file_path, self.config["TOPIC_NAME"])

    def send_csv_files_to_kafka(self, csv_file: str, topic_name: str):
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            trade = row.to_dict()
            json_data = json.dumps(trade)
            self.kafka_producer.send(topic_name, key=trade['symbol'], value=json_data)

        self.kafka_producer.flush()
        print(f"All rows from {csv_file} have been sent to {topic_name}")
