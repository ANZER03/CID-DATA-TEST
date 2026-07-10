# Stage 1: grab Spark binaries from the official image
FROM apache/spark:3.5.3 AS spark-base

# Stage 2: Airflow image enriched with Spark + JARs
FROM apache/airflow:2.7.1

USER root

# Java is required to run spark-submit
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copy Spark distribution from the spark image
COPY --from=spark-base /opt/spark /opt/spark

# Overlay our pre-downloaded JARs (Iceberg, Nessie, S3A, Kafka…)
COPY jars/*.jar /opt/spark/jars/

# Spark config: Nessie catalog + MinIO S3A endpoint
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# PySpark scripts
COPY scripts/ /opt/airflow/scripts/

# DAGs
COPY dags/ /opt/airflow/dags/

# Fix spark dir ownership so airflow user can write logs etc.
RUN chown -R airflow: /opt/spark/work-dir 2>/dev/null || true

USER airflow
