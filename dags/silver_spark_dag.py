from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SILVER_SCRIPT = "/opt/airflow/scripts/silver_transform.py"

default_args = {
    'owner': 'AB SOUFIANE',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='2_SILVER_TRANSFORMATION_SPARK',
    default_args=default_args,
    description='Silver layer: clean + deduplicate via PySpark (replaces dbt)',
    schedule_interval=None,
    start_date=datetime(2026, 5, 9),
    catchup=False,
    tags=['pfe', 'silver', 'spark'],
) as dag:

    silver_spark = BashOperator(
        task_id='silver_spark_submit',
        bash_command=f'spark-submit {SILVER_SCRIPT}',
    )
