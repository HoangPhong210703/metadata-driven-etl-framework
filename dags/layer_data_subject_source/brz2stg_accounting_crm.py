from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="brz2stg_accounting_crm",
    description="Button: trigger bronze-to-staging for accounting from crm source",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["button", "brz2stg", "accounting", "postgres_crm"],
) as dag:
    
    # Trigger the brz2stg coordinator DAG
    trigger_coor = TriggerDagRunOperator(
        task_id="trigger_brz2stg_coordinator",
        trigger_dag_id="coordinator",
        conf={"layer": "brz2stg", "data_subject": "accounting", "source": "postgres_crm"},
        wait_for_completion=False,
    )
