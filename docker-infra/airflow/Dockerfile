FROM apache/airflow:2.7.1

USER root
# Install system packages required for dbt-spark
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    libsasl2-dev \
    python3-dev \
    libssl-dev \
    libsasl2-modules \
    && apt-get clean

# Git permissions workaround
RUN git config --global --add safe.directory /opt/airflow/dbt

USER airflow
# Install dbt-spark with Nessie support
RUN pip install --no-cache-dir \
    "dbt-spark[PyHive]==1.7.1" \
    "dbt-nessie==0.21.1"

# Copy DAGs, dbt project and scripts into the image (no bind mounts needed)
COPY dags/ /opt/airflow/dags/
COPY dbt_project/ /opt/airflow/dbt/
COPY scripts/ /opt/airflow/scripts/
