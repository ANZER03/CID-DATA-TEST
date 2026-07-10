from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

TEST_SCRIPT = "/opt/airflow/scripts/test_bucketing_partition.py"

default_args = {
    'owner': 'AB SOUFIANE',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='3_TEST_BUCKETING_PARTITION_SPARK',
    default_args=default_args,
    description='Test job: aggregate transactions bucketed by client and partitioned by hour in s3a://test/',
    schedule_interval=None,
    start_date=datetime(2026, 5, 9),
    catchup=False,
    tags=['pfe', 'test', 'spark'],
) as dag:

    test_spark = BashOperator(
        task_id='test_spark_submit',
        bash_command=f'spark-submit {TEST_SCRIPT}',
    )
