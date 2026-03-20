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


DBT_TEST_TABLE = "dbt_test_results"


def _ensure_dbt_test_table(conn):
    """Create dbt test results table if it doesn't exist."""
    from sqlalchemy import text

    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.{DBT_TEST_TABLE} (
            id SERIAL PRIMARY KEY,
            run_id TEXT,
            test_name TEXT,
            status TEXT,
            failures INTEGER,
            execution_time FLOAT,
            message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))


def log_dbt_results(results: list[dict], run_id: str) -> None:
    """Write dbt test results to the warehouse."""
    from sqlalchemy import create_engine, text

    if not results:
        return

    engine = create_engine(_get_warehouse_url())
    with engine.begin() as conn:
        _ensure_dbt_test_table(conn)
        for r in results:
            conn.execute(
                text(f"""
                    INSERT INTO {AUDIT_SCHEMA}.{DBT_TEST_TABLE}
                        (run_id, test_name, status, failures, execution_time, message)
                    VALUES
                        (:run_id, :test_name, :status, :failures, :execution_time, :message)
                """),
                {
                    "run_id": run_id,
                    "test_name": r["test_name"],
                    "status": r["status"],
                    "failures": r["failures"],
                    "execution_time": r["execution_time"],
                    "message": r.get("message"),
                },
            )
    engine.dispose()
    print(f"[audit] Wrote {len(results)} dbt test results to {AUDIT_SCHEMA}.{DBT_TEST_TABLE}")


FRESHNESS_TABLE = "freshness_check_results"


def _ensure_freshness_table(conn):
    """Create freshness check results table if it doesn't exist."""
    from sqlalchemy import text

    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.{FRESHNESS_TABLE} (
            id SERIAL PRIMARY KEY,
            source_name TEXT,
            data_subject TEXT,
            status TEXT,
            max_stale_hours INTEGER,
            hours_since_load FLOAT,
            last_loaded_at TIMESTAMPTZ,
            checked_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))


def log_freshness_results(results: list[dict]) -> None:
    """Write freshness check results to the warehouse."""
    from sqlalchemy import create_engine, text

    if not results:
        return

    engine = create_engine(_get_warehouse_url())
    with engine.begin() as conn:
        _ensure_freshness_table(conn)
        for r in results:
            conn.execute(
                text(f"""
                    INSERT INTO {AUDIT_SCHEMA}.{FRESHNESS_TABLE}
                        (source_name, data_subject, status, max_stale_hours,
                         hours_since_load, last_loaded_at)
                    VALUES
                        (:source_name, :data_subject, :status, :max_stale_hours,
                         :hours_since_load, :last_loaded_at)
                """),
                r,
            )
    engine.dispose()
    print(f"[audit] Wrote {len(results)} freshness results to {AUDIT_SCHEMA}.{FRESHNESS_TABLE}")
