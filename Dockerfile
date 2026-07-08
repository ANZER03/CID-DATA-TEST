FROM apache/airflow:2.7.1

USER root
# dbt-spark
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    libsasl2-dev \
    python3-dev \
    libssl-dev \
    libsasl2-modules \
    && apt-get clean

# حل مشكل Permission ديال Git
RUN git config --global --add safe.directory /opt/airflow/dbt

USER airflow
# install dbt-spark 
RUN pip install --no-cache-dir "dbt-spark[PyHive]==1.7.1"