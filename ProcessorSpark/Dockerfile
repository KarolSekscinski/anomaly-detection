FROM bitnami/spark:3.5.0

# Set the working directory
WORKDIR /app

# Install Python and pip if not already installed
USER root
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y python3 python3-pip
#    apt-get install -y python3 python3-pip && \
#    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application-specific JARs
COPY spark/target/dependency /opt/bitnami/spark/jars

# Set environment variables for Spark application arguments
ENV SPARK_APPLICATION_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0, \
                            com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.datastax.oss:java-driver-core:4.0.0"

# Copy the Python application
COPY src /app/src

# Set the Python application location as an environment variable
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/src/main/python/StreamData.py

# Entry point
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]

# Use shell form to ensure variables are resolved
CMD spark-submit --master local[*] --deploy-mode client "$SPARK_APPLICATION_PYTHON_LOCATION" $SPARK_APPLICATION_ARGS
