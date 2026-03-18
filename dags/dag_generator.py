from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore

with DAG(
    dag_id="dag_generator",
    description="Generates DAGs from the dag_config.csv file",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["generator"],
) as dag:
    generate_dags = BashOperator(
        task_id="generate_dags_from_config",
        bash_command="python /opt/airflow/dags/dag_init_script.py",
    )
