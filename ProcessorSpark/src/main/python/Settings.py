import configparser
import ast


class Settings:
    def __init__(self, config_file='/app/src/main/resources/application.conf'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        self.cassandra = {
            "host": self.config.get('cassandra', 'host'),
            "port": self.config.get('cassandra', 'port'),
            "keyspace": self.config.get('cassandra', 'keyspace'),
            "username": self.config.get('cassandra', 'username'),
            "password": self.config.get('cassandra', 'password'),
            "tables": {
                "trades": self.config.get('cassandra.tables', 'trades'),
                "anomalies": self.config.get('cassandra.tables', 'anomalies')
            }

        }

        self.kafka = {
            "server_address": self.config.get('kafka', 'server_address'),
            "topic": [
                {
                    "market": self.config.get('kafka.topics', 'market')
                }
            ],
            "min_partitions": [
                {
                    "StreamProcessor": self.config.get('kafka.min_partitions', 'StreamProcessor')
                }
            ]
        }

        self.spark = {
            "master": self.config.get('spark', 'master'),
            "appName": [
                {
                    "StreamProcessor": self.config.get('spark.appName', 'StreamProcessor')
                }
            ],
            "max_offsets_per_trigger": [
                {
                    "StreamProcessor": self.config.get('spark.max_offsets_per_trigger', 'StreamProcessor')
                }
            ],
            "shuffle_partitions": [
                {
                    "StreamProcessor": self.config.get('spark.shuffle_partitions', 'StreamProcessor')
                }
            ],
            "deprecated_offsets": [
                {
                    "StreamProcessor": self.config.get('spark.deprecated_offsets', 'StreamProcessor')
                }
            ]
        }

        self.schemas = {
            "trades": self.config.get('schemas', 'trades')
        }

        self.anomalies = {
            "window_sizes": {
                "prices": ast.literal_eval(self.config.get('anomalies.window_size', 'prices')),
                "volumes": ast.literal_eval(self.config.get('anomalies.window_size', 'volumes'))

            },
            "thresholds": {
                "z-threshold": self.config.get('anomalies.thresholds', 'z-threshold'),
                "pv-threshold": self.config.get('anomalies.thresholds', 'pv-threshold')
            },
            #"contamination_factors": ast.literal_eval(self.config.get('anomalies.contamination_factors',
                                                                      # 'contamination_factors'))
        }
