from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Laurent',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'capstonellm_pipeline',
    default_args=default_args,
    description='A DAG to run capstonellm tasks sequentially',
    schedule=None,
    catchup=False,
)

# Task 1: Ingest
ingest_task = BashOperator(
    task_id='ingest_data',
    bash_command='capstonellm.tasks.ingest',
    dag=dag,
)

# Task 2: Clean
clean_task = BashOperator(
    task_id='clean_data',
    bash_command='capstonellm.tasks.clean',
    dag=dag,
)

# Set task dependencies
ingest_task >> clean_task