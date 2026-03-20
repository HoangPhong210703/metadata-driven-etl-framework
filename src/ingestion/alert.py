"""Email alerting — sends notifications for pipeline completion/failure and stale data."""

import csv
import os
from pathlib import Path

ALERT_CONFIG_PATH = Path("/opt/airflow/config/alert_config.csv")


def _get_recipients(alert_type: str) -> list[str]:
    """Read recipients for a given alert type from alert_config.csv."""
    if not ALERT_CONFIG_PATH.exists():
        return []
    with open(ALERT_CONFIG_PATH) as f:
        for row in csv.DictReader(f):
            if row["alert_type"] == alert_type and row.get("active", "1").strip() == "1":
                return [r.strip() for r in row["recipients"].split(";") if r.strip()]
    return []


def send_alert(alert_type: str, subject: str, body: str) -> None:
    """Send email alert using Airflow's SMTP configuration.

    Falls back to logging if SMTP is not configured.
    """
    recipients = _get_recipients(alert_type)
    if not recipients:
        print(f"[alert] No active recipients for alert_type='{alert_type}', skipping")
        return

    smtp_host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "")
    if not smtp_host:
        print(f"[alert] SMTP not configured — would send '{subject}' to {recipients}")
        print(f"[alert] Body: {body}")
        return

    try:
        from airflow.utils.email import send_email
        send_email(
            to=recipients,
            subject=f"[ETL Pipeline] {subject}",
            html_content=f"<pre>{body}</pre>",
        )
        print(f"[alert] Sent '{subject}' to {recipients}")
    except Exception as e:
        print(f"[alert] WARNING: Failed to send email: {e}")
        print(f"[alert] Subject: {subject}")
        print(f"[alert] Body: {body}")


def notify_dag_status(**kwargs) -> None:
    """Send one email summarizing the entire DAG run (success or failure).

    Intended as the final task in an execution DAG with trigger_rule='all_done'.
    """
    from airflow.utils.state import TaskInstanceState

    dag_run = kwargs["dag_run"]
    dag_id = dag_run.dag_id
    conf = dag_run.conf or {}
    source = conf.get("source", "")
    data_subject = conf.get("data_subject", "")

    # Check status of all other tasks in this run
    tis = dag_run.get_task_instances()
    failed = [ti for ti in tis if ti.state == TaskInstanceState.FAILED]
    succeeded = [ti for ti in tis if ti.state == TaskInstanceState.SUCCESS]
    # Exclude the notify task itself
    current_task_id = kwargs["ti"].task_id
    failed = [ti for ti in failed if ti.task_id != current_task_id]
    succeeded = [ti for ti in succeeded if ti.task_id != current_task_id]

    context = f"{data_subject}/{source}" if source else dag_id

    if failed:
        failed_names = ", ".join(ti.task_id for ti in failed)
        body = (
            f"DAG: {dag_id}\n"
            f"Run: {dag_run.run_id}\n"
            f"Source: {context}\n\n"
            f"Failed tasks: {failed_names}\n"
            f"Succeeded tasks: {', '.join(ti.task_id for ti in succeeded)}\n\n"
            f"Check Airflow UI for details."
        )
        send_alert(
            alert_type="pipeline_failure",
            subject=f"FAILED: {dag_id} ({context})",
            body=body,
        )
    else:
        body = (
            f"DAG: {dag_id}\n"
            f"Run: {dag_run.run_id}\n"
            f"Source: {context}\n\n"
            f"All {len(succeeded)} tasks completed successfully."
        )
        send_alert(
            alert_type="pipeline_success",
            subject=f"SUCCESS: {dag_id} ({context})",
            body=body,
        )
