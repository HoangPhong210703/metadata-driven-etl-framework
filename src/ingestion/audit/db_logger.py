"""Database audit logging — writes records to meta.pipeline_audit in the warehouse."""

from pathlib import Path

SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
AUDIT_SCHEMA = "meta"
AUDIT_TABLE = "pipeline_audit"


def _get_warehouse_url() -> str:
    import tomllib
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def _ensure_audit_table(conn):
    """Create audit schema and table if they don't exist."""
    from sqlalchemy import text

    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.{AUDIT_TABLE} (
            id SERIAL PRIMARY KEY,
            run_id TEXT,
            dag_id TEXT,
            task_id TEXT,
            layer TEXT,
            source TEXT,
            data_subject TEXT,
            table_name TEXT,
            status TEXT NOT NULL,
            row_count INTEGER,
            error_message TEXT,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ
        )
    """))


def log_to_db(record: dict):
    """Write a single audit record to the warehouse."""
    from sqlalchemy import create_engine, text

    engine = create_engine(_get_warehouse_url())
    with engine.begin() as conn:
        _ensure_audit_table(conn)
        conn.execute(
            text(f"""
                INSERT INTO {AUDIT_SCHEMA}.{AUDIT_TABLE}
                    (run_id, dag_id, task_id, layer, source, data_subject,
                     table_name, status, row_count, error_message, started_at, finished_at)
                VALUES
                    (:run_id, :dag_id, :task_id, :layer, :source, :data_subject,
                     :table_name, :status, :row_count, :error_message, :started_at, :finished_at)
            """),
            record,
        )
    engine.dispose()
