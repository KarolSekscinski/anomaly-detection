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
    PRIMARY KEY ( (symbol), ingestion_ts )
)
WITH CLUSTERING ORDER BY (ingestion_ts desc);

CREATE INDEX IF NOT EXISTS ON stocks (uuid);

CREATE TABLE price_anomalies(
    uuid uuid,
    price double,
    trade_ts timestamp,
    symbol text,
    ingestion_ts timestamp,
    detection_ts timestamp,
    PRIMARY KEY ( (symbol), ingestion_ts )
)
WITH CLUSTERING ORDER BY (ingestion_ts desc);
CREATE INDEX IF NOT EXISTS ON price_anomalies (symbol);

CREATE TABLE volume_anomalies(
    uuid uuid,
    volume double,
    trade_ts timestamp,
    symbol text,
    ingestion_ts timestamp,
    detection_ts timestamp,
    PRIMARY KEY ( (symbol), ingestion_ts )
)
WITH CLUSTERING ORDER BY (ingestion_ts desc);
CREATE INDEX IF NOT EXISTS ON volume_anomalies (symbol);

