"""@audited decorator — wraps Airflow task callables with audit logging."""

import traceback
from datetime import datetime, timezone
from functools import wraps

from src.ingestion.audit import log_audit


def _set_dagrun_note(dag_run, conf):
    """Set a note on the DagRun visible in Airflow Grid view."""
    try:
        from airflow.utils.session import create_session

        button = conf.get("button", "")
        source = conf.get("source", "")
        data_subject = conf.get("data_subject", "")

        if button:
            note = f"Button: {button}"
        elif source and data_subject:
            note = f"{data_subject} | {source}"
        else:
            return

        from airflow.models.dagrun import DagRun

        with create_session() as session:
            dr = session.query(DagRun).filter(
                DagRun.dag_id == dag_run.dag_id,
                DagRun.run_id == dag_run.run_id,
            ).first()
            if dr:
                dr.note = note
                session.commit()
    except Exception as e:
        print(f"[audit] WARNING: Failed to set DagRun note: {e}")


def audited(func):
    """Decorator that wraps an Airflow PythonOperator callable with audit logging.

    Extracts dag_id, task_id, run_id from Airflow kwargs.
    Extracts layer, source, data_subject from dag_run.conf.
    Sets a DagRun note for visibility in the Airflow UI.
    Logs success or failure to both file and database.
    """
    @wraps(func)
    def wrapper(**kwargs):
        ti = kwargs.get("ti")
        dag_run = kwargs.get("dag_run")
        conf = dag_run.conf or {} if dag_run else {}

        dag_id = ti.dag_id if ti else "unknown"
        task_id = ti.task_id if ti else "unknown"
        run_id = dag_run.run_id if dag_run else "unknown"

        layer = conf.get("layer")
        source = conf.get("source")
        data_subject = conf.get("data_subject")

        # Set DagRun note for UI visibility
        _set_dagrun_note(dag_run, conf)

        started_at = datetime.now(timezone.utc)
        try:
            result = func(**kwargs)

            # Extract row_count from return value if available
            row_count = None
            if isinstance(result, dict):
                row_count = result.get("row_count") or result.get("total")

            log_audit(
                run_id=run_id,
                dag_id=dag_id,
                task_id=task_id,
                status="success",
                layer=layer,
                source=source,
                data_subject=data_subject,
                row_count=row_count,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
            return result
        except Exception as e:
            log_audit(
                run_id=run_id,
                dag_id=dag_id,
                task_id=task_id,
                status="failed",
                layer=layer,
                source=source,
                data_subject=data_subject,
                error_message=f"{type(e).__name__}: {e}\n{traceback.format_exc()}",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
            raise

    return wrapper
