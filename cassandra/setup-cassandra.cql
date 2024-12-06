DROP KEYSPACE IF EXISTS market;

CREATE KEYSPACE market WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE market;

CREATE TABLE stocks(
    uuid uuid,
    trade_conditions text,
    price double,
    symbol text,
    trade_ts timestamp,
    ingestion_ts timestamp,
    volume double,
    type text,
    PRIMARY KEY ( (symbol), trade_ts )
);

CREATE INDEX IF NOT EXISTS ON stocks (uuid);

CREATE TABLE anomalies(
    uuid uuid,
    symbol text,
    price double,
    volume double,
    anomaly_type text,
    ingestion_ts timestamp,
    time_to_detect double,
    PRIMARY KEY ( (uuid), ingestion_ts )
);

CREATE INDEX IF NOT EXISTS ON anomalies (symbol);