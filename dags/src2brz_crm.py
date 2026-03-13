"""Button DAG — triggers coordinator sensor for crm data subject."""

from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore

with DAG(
    dag_id="src2brz_crm",
    description="Button: trigger source-to-bronze for crm",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["button", "src2brz", "crm"],
) as dag:
    EmptyOperator(task_id="signal_coordinator")
