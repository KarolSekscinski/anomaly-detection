FROM bde2020/spark-python-template:3.2.0-hadoop3.2

# Download Spark packages
COPY spark/target/dependency /spark/jars

ENV SPARK_APPLICATION_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12-3.2.0,org.apache.spark:spark-avro_2.12:3.2.0, \
                            spark-cassandra-connector_2.12-3.2.0,java-driver-core-4.0.0"

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/src/main/python/MainProcessor.py