from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    'owner': 'AB SOUFIANE',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='2_SILVER_TRANSFORMATION_DBT',
    default_args=default_args,
    description='Pipeline to clean data and remove duplicates using dbt',
    schedule_interval=None, 
    start_date=datetime(2026, 5, 9),
    catchup=False,
    tags=['pfe', 'silver', 'dbt'],
) as dag:

 
    dbt_debug = BashOperator(
        task_id='dbt_debug_connection',
        bash_command=f'dbt debug --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )


    dbt_run = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )


    dbt_debug >> dbt_run