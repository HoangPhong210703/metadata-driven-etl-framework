"""Button DAG — triggers coordinator sensor for company data subject."""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="src2brz_company",
    description="Button: trigger source-to-bronze for company",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["button", "src2brz", "company"],
) as dag:
    EmptyOperator(task_id="signal_coordinator")
