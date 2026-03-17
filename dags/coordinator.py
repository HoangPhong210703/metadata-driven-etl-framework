import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.exceptions import AirflowSkipException  # type: ignore


def coor(**kwargs):
    dag_run = kwargs["dag_run"]

    conf = dag_run.conf or {}
    button = conf.get("button")

    if not button:
        raise AirflowSkipException("No button dag id provided in conf")

    parts = button.split("__")
    if len(parts) != 3:
        raise AirflowSkipException(f"Invalid button dag id format: {button} (expected layer__subject__source)")

    layer, subject, source = parts
    print(f"[coordinator] Parsed button '{button}' → layer={layer}, subject={subject}, source={source}")

    config = {
        "layer": layer,
        "data_subjects": [subject],
        "source": source,
        "target_dag": f"{layer}__get_config",
    }

    return config



with DAG(
    dag_id="coordinator",
    description="Orchestrates source-to-bronze pipeline (Triggered by Button DAGs)",
    schedule=None,  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:

    coor_task = PythonOperator(
        task_id="coor",
        python_callable=coor,
    )

    get_config_trigger = TriggerDagRunOperator(
        task_id="get_config_trigger",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='coor')['target_dag'] }}",
        conf="{{ ti.xcom_pull(task_ids='coor') }}",
    )

    coor_task >> get_config_trigger