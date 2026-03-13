"""Coordinator DAG — watches button DAGs via sensors, handles errors,
triggers the get_config → processing → ingestion chain."""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore
from airflow.sensors.time_delta import TimeDeltaSensor  # type: ignore

sys.path.insert(0, "/opt/airflow")

from src.ingestion.config import load_csv_config, get_data_subjects

CONFIG_PATH = Path("/opt/airflow/config/src2brz_config.csv")
SENSOR_DEADLINE = 60 * 60  # 1 hour


def _recent_run(external_dag_id):
    """Find a successful run of the external DAG from the last 60 seconds."""
    def fn(logical_date, **kwargs):
        from airflow.models import DagRun # type: ignore
        from airflow.utils import timezone # type: ignore
        now = timezone.utcnow()
        cutoff = now - timedelta(seconds=60)
        runs = DagRun.find(
            dag_id=external_dag_id,
            execution_start_date=cutoff,
            execution_end_date=now,
        )
        successful = [r for r in runs if r.state == "success"]
        if successful:
            successful.sort(key=lambda r: r.execution_date, reverse=True)
            return successful[0].execution_date
        return logical_date
    return fn


def coor(**kwargs):
    """Check which button sensors passed, log results, push active subjects."""
    dag_run = kwargs["dag_run"]
    active_subjects = []

    for ti in dag_run.get_task_instances():
        if ti.task_id.startswith("wait_src2brz_"):
            ds = ti.task_id.replace("wait_src2brz_", "")
            if ti.state == "success":
                active_subjects.append(ds)
                print(f"[coordinator] {ti.task_id}: OK")
            else:
                print(f"[coordinator] {ti.task_id}: {ti.state} — skipping {ds}")

    if not active_subjects:
        from airflow.exceptions import AirflowSkipException # type: ignore
        raise AirflowSkipException("No button DAGs completed — nothing to process")

    config = {"layer": "src2brz", "data_subjects": sorted(active_subjects)}
    print(f"[coordinator] Active data subjects: {config['data_subjects']}")
    return config


# --- Build DAG ---
_configs = load_csv_config(CONFIG_PATH) if CONFIG_PATH.exists() else []
_data_subjects = get_data_subjects(_configs)

with DAG(
    dag_id="coordinator",
    description="Orchestrates source-to-bronze pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:
    # Sensors watch button DAGs — timeout matches wait_window so they
    # all finish (succeed or soft_fail) by the time coor runs.
    WAIT_WINDOW_SECONDS = 60
    sensors = []
    for ds in sorted(_data_subjects):
        sensor = ExternalTaskSensor(
            task_id=f"wait_src2brz_{ds}",
            external_dag_id=f"src2brz_{ds}",
            external_task_id="signal_coordinator",
            mode="reschedule",
            timeout=WAIT_WINDOW_SECONDS,
            poke_interval=10,
            soft_fail=True,
            execution_date_fn=_recent_run(f"src2brz_{ds}"),
        )
        sensors.append(sensor)

    # Timer matches sensor timeout — when it finishes, all sensors are done
    wait_window = TimeDeltaSensor(
        task_id="wait_window",
        delta=timedelta(seconds=WAIT_WINDOW_SECONDS),
        mode="reschedule",
        poke_interval=10,
    )

    coor_task = PythonOperator(
        task_id="coor",
        python_callable=coor,
        # Runs when the timer finishes — doesn't wait for all sensors
        trigger_rule="one_success",
    )

    get_config_trigger = TriggerDagRunOperator(
        task_id="get_config_trigger",
        trigger_dag_id="get_config",
        conf="{{ ti.xcom_pull(task_ids='coor') }}",
    )

    # Sensors and timer all feed into coor
    for sensor in sensors:
        sensor >> coor_task
    wait_window >> coor_task
    coor_task >> get_config_trigger
