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
)
WITH CLUSTERING ORDER BY (trade_ts desc);

CREATE INDEX IF NOT EXISTS ON stocks (uuid);

CREATE TABLE anomalies(
    uuid uuid,
    price double,
    trade_ts timestamp,
    volume double,
    symbol text,
    window_size int,
    threshold double,
    ingestion_ts timestamp,
    PRIMARY KEY ( (uuid), ingestion_ts )
);

CREATE INDEX IF NOT EXISTS ON anomalies (uuid);

