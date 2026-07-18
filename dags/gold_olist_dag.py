"""
gold_olist_dag.py
-----------------
Airflow DAG — triggers the Olist Gold PySpark job via spark-submit.

Trigger: manual (schedule_interval=None)
Run AFTER 3_SILVER_OLIST has completed successfully.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

GOLD_SCRIPT = "/opt/airflow/scripts/gold_olist.py"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="4_GOLD_OLIST",
    default_args=default_args,
    description=(
        "Silver → Gold: build analytics aggregations "
        "(daily sales, monthly sales, category performance) "
        "as Iceberg tables in s3a://gold/"
    ),
    schedule_interval=None,
    start_date=datetime(2026, 7, 18),
    catchup=False,
    tags=["olist", "gold", "spark", "iceberg"],
) as dag:

    gold_job = BashOperator(
        task_id="spark_submit_gold_olist",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--driver-memory 1g "
            f"{GOLD_SCRIPT}"
        ),
    )
