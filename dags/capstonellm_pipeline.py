from datetime import datetime, timedelta
from airflow import DAG
from conveyor.operators import ConveyorContainerOperatorV2


# Define default arguments for the DAG
default_args = {
    'owner': 'laurent_bonesire',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'capstonellm_docker_pipeline',
    default_args=default_args,
    description='A DAG to run capstonellm tasks in Conveyor',
    schedule=None,
    catchup=False,
    tags=['capstonellm', 'pipeline', 'docker'],
)

# Task 1: Ingest (runs in a Docker container)
ingest_task = ConveyorContainerOperatorV2(
    dag=dag,
    task_id="ingest_data",
    instance_type="mx.medium",
    aws_role="capstone_conveyor_llm",
    cmds=["uv", "run", "python3", "-m", "capstonellm.tasks.ingest"],
    #arguments=[""],
)
'''
OLD DOCKER EXAMPLE
ConveyorContainerOperatorV2(
    task_id='ingest_data',
    image='capstonellm:latest',  # Replace with your actual image name
    command='uv run python3 -m capstonellm.tasks.ingest',
    docker_url='unix://var/run/docker.sock',  # Default for local Docker
    network_mode='bridge',
    mount_tmp_dir=False,  # Airflow 3: Explicitly disable tmp dir mounting if not needed
    dag=dag,
)
'''
# Task 2: Clean (runs in a Docker container, regardless of ingest success)
clean_task = ConveyorContainerOperatorV2(
    dag=dag,
    task_id="clean_data",
    instance_type="mx.medium",
    aws_role="capstone_conveyor_llm",
    cmds=["uv", "run", "python3", "-m", "capstonellm.tasks.ingest"],
    #arguments=["your_container_arguments"],
)
'''
OLD DOCKER EXAMPLE
ConveyorContainerOperatorV2(
    task_id='clean_data',
    image='capstonellm:latest',  # Same image as ingest_task
    command='uv run python3 -m capstonellm.tasks.clean',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    mount_tmp_dir=False,
    trigger_rule='all_done',  # Run regardless of ingest_task's success/failure
    dag=dag,
)
'''
# Set task dependencies
ingest_task >> clean_task
