"""
silver_olist_dag.py
-------------------
Airflow DAG — triggers the Olist Silver PySpark job via spark-submit.

Trigger: manual (schedule_interval=None)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SILVER_SCRIPT = "/opt/airflow/scripts/silver_olist.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="3_SILVER_OLIST",
    default_args=default_args,
    description=(
        "Bronze → Silver: clean, type-cast, validate, deduplicate "
        "all Olist CSV files and write Iceberg tables to s3a://silver/"
    ),
    schedule_interval=None,
    start_date=datetime(2026, 7, 18),
    catchup=False,
    tags=["olist", "silver", "spark", "iceberg"],
) as dag:

    silver_job = BashOperator(
        task_id="spark_submit_silver_olist",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--driver-memory 1g "
            f"{SILVER_SCRIPT}"
        ),
    )
