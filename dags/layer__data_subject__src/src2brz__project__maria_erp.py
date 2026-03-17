from datetime import datetime

from airflow import DAG #type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type: ignore

DAG_ID = "src2brz__project__maria_erp"

with DAG(
    dag_id=DAG_ID,
    description="Button: trigger source-to-bronze for project from maria_erp",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["button", "src2brz", "project", "maria_erp"],
) as dag:

    trigger_coor = TriggerDagRunOperator(
        task_id="trigger_coordinator",
        trigger_dag_id="coordinator",
        conf={"button": DAG_ID},
        wait_for_completion=False,
    )
