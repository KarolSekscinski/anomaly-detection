FROM bitnami/spark:3.5.0


# Copy application-specific JARs
COPY jars/target/dependency /opt/bitnami/spark/jars

COPY requirements.txt .

USER root

RUN apt-get clean  && \
    apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install -r ./requirements.txt