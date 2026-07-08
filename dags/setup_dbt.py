from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='0_A_SETUP_DBT_ENVIRONMENT',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    install_dbt = BashOperator(
        task_id='install_dbt_packages',
        bash_command='pip install --no-cache-dir dbt-spark==1.7.0 dbt-core==1.7.0 pyhive thrift thrift-sasl pure-sasl'
    )