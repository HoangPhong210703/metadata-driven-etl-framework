"""Email alerting — sends notifications for pipeline completion/failure and stale data."""

import csv
import os
import traceback
from pathlib import Path

ALERT_CONFIG_PATH = Path("/opt/airflow/config/alert_config.csv")


def _get_recipients(alert_type: str) -> list[str]:
    """Read recipients for a given alert type from alert_config.csv."""
    if not ALERT_CONFIG_PATH.exists():
        return []
    recipients = []
    with open(ALERT_CONFIG_PATH) as f:
        for row in csv.DictReader(f):
            if row["alert_type"] == alert_type and row.get("active", "1").strip() == "1":
                emails = [r.strip() for r in row["recipients"].replace(",", ";").split(";") if r.strip()]
                recipients.extend(emails)
    return list(set(recipients))


def send_alert(alert_type: str, subject: str, body: str) -> None:
    """Send email alert using yagmail with Gmail credentials from .env.

    Falls back to logging if SENDER_EMAIL/PASSWORD are not configured.
    """
    recipients = _get_recipients(alert_type)
    print(f"[alert] Found {len(recipients)} recipients for alert_type='{alert_type}': {recipients}")
    if not recipients:
        print(f"[alert] No active recipients for alert_type='{alert_type}', skipping")
        return

    sender_email = os.environ.get("SENDER_EMAIL")
    sender_password = os.environ.get("SENDER_PASSWORD")

    if not sender_email or not sender_password:
        print(f"[alert] SENDER_EMAIL or SENDER_PASSWORD not found in .env — would send '{subject}' to {recipients}")
        print(f"[alert] Body: {body}")
        return

    try:
        import yagmail
        import keyring
        from keyring.backends import null
        keyring.set_keyring(null.Keyring())
        print(f"[alert] Initializing SMTP with user '{sender_email}'...")
        yag = yagmail.SMTP(sender_email, sender_password)
        print(f"[alert] Sending '{subject}' to {recipients}...")
        yag.send(
            to=recipients,
            subject=f"[ETL Pipeline] {subject}",
            contents=f"<pre>{body}</pre>",
        )
        print(f"[alert] Successfully sent email to {recipients}")
    except Exception as e:
        print(f"[alert] ERROR: Failed to send email. See traceback below.")
        print(f"[alert] Subject: {subject}")
        # Print the full traceback
        traceback.print_exc()


def dag_failure_callback(context) -> None:
    """DAG-level callback for pipeline failure."""
    from airflow.utils.state import TaskInstanceState

    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    conf = dag_run.conf or {}
    source = conf.get("source", "")
    data_subject = conf.get("data_subject", "")

    tis = dag_run.get_task_instances()
    failed = [ti for ti in tis if ti.state == TaskInstanceState.FAILED]

    ctx_str = f"{data_subject}/{source}" if source else dag_id
    failed_names = ", ".join(ti.task_id for ti in failed) if failed else "Unknown (Check UI)"

    body = (
        f"DAG: {dag_id}\n"
        f"Run: {dag_run.run_id}\n"
        f"Source: {ctx_str}\n\n"
        f"Failed tasks: {failed_names}\n\n"
        f"Check Airflow UI for details."
    )
    send_alert(
        alert_type="pipeline_failure",
        subject=f"FAILED: {dag_id} ({ctx_str})",
        body=body,
    )


def dag_success_callback(context) -> None:
    """DAG-level callback for pipeline success."""
    from airflow.utils.state import TaskInstanceState

    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    conf = dag_run.conf or {}
    source = conf.get("source", "")
    data_subject = conf.get("data_subject", "")

    tis = dag_run.get_task_instances()
    succeeded = [ti for ti in tis if ti.state == TaskInstanceState.SUCCESS]

    ctx_str = f"{data_subject}/{source}" if source else dag_id

    body = (
        f"DAG: {dag_id}\n"
        f"Run: {dag_run.run_id}\n"
        f"Source: {ctx_str}\n\n"
        f"All {len(succeeded)} tasks completed successfully."
    )
    send_alert(
        alert_type="pipeline_success",
        subject=f"SUCCESS: {dag_id} ({ctx_str})",
        body=body,
    )
