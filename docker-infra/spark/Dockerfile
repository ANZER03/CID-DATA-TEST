FROM apache/spark:3.5.3

USER root

# Copy all pre-downloaded JAR files from the host into the image
COPY jars/*.jar /opt/spark/jars/

# Copy Spark configuration and scripts into the image
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY scripts/ingest_bronze.py /opt/spark/work/scripts/ingest_bronze.py
COPY scripts/test_kafka_spark.py /opt/spark/work/scripts/test_kafka_spark.py

# Create checkpoint directory and fix permissions
RUN mkdir -p /tmp/spark-checkpoints && chown -R spark:spark /tmp/spark-checkpoints

# Fix permissions
RUN chmod 644 /opt/spark/jars/*.jar

USER spark
