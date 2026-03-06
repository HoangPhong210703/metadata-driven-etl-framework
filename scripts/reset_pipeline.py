"""Reset pipeline state interactively. Run from project root: python scripts/reset_pipeline.py"""

import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SECRETS_PATH = PROJECT_ROOT / ".dlt" / "secrets.toml"


def ask(question: str) -> bool:
    answer = input(f"  {question} [y/N] ").strip().lower()
    return answer == "y"


def load_warehouse_credentials() -> str:
    with open(SECRETS_PATH, "rb") as f:
        raw = tomllib.load(f)
    return raw["destinations"]["warehouse"]["credentials"]


def drop_schema(conn, schema: str) -> None:
    conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    print(f"    Dropped schema: {schema}")


def clear_bronze_parquet() -> None:
    if not BRONZE_DIR.exists():
        print("    Bronze directory does not exist, nothing to clear.")
        return
    shutil.rmtree(BRONZE_DIR)
    BRONZE_DIR.mkdir(parents=True)
    print(f"    Cleared: {BRONZE_DIR}")


def clear_dlt_destination_state() -> None:
    count = 0
    for state_dir in BRONZE_DIR.rglob("_dlt_pipeline_state"):
        shutil.rmtree(state_dir)
        count += 1
    print(f"    Removed {count} _dlt_pipeline_state folder(s) from bronze.")


def clear_dlt_local_state() -> None:
    result = subprocess.run(
        ["docker", "compose", "exec", "airflow-worker",
         "bash", "-c", "rm -rf ~/.dlt/pipelines/ && echo done"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print("    Cleared dlt local state inside Docker worker.")
    else:
        print(f"    WARNING: Could not clear Docker dlt state: {result.stderr.strip()}")


def clear_postgres_schemas(schemas: list[str]) -> None:
    try:
        import psycopg2
        from urllib.parse import urlparse

        creds = load_warehouse_credentials()
        parsed = urlparse(creds)
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=parsed.username,
            password=parsed.password,
            dbname=(parsed.path or "").lstrip("/"),
        )
        conn.autocommit = True
        cur = conn.cursor()
        for schema in schemas:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"    Dropped schema: {schema}")
        cur.close()
        conn.close()
    except ImportError:
        print("    ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    except Exception as e:
        print(f"    ERROR: {e}")


def main():
    print("\n=== Pipeline Reset Tool ===\n")
    print("Select what to clear:\n")

    clear_parquet = ask("Bronze parquet files (data/bronze/)?")
    clear_dest_state = ask("dlt destination state (_dlt_pipeline_state/ folders in bronze)?")
    clear_local_state = ask("dlt local state inside Docker worker (~/.dlt/pipelines/)?")
    clear_stg_temp = ask("stg_temp schema in Postgres?")
    clear_stg = ask("stg schema in Postgres?")
    clear_silver = ask("silver schema in Postgres?")

    nothing = not any([clear_parquet, clear_dest_state, clear_local_state,
                       clear_stg_temp, clear_stg, clear_silver])
    if nothing:
        print("\nNothing selected. Exiting.")
        return

    print("\n--- Executing ---\n")

    if clear_parquet:
        print("  Clearing bronze parquet files...")
        clear_bronze_parquet()

    if clear_dest_state:
        print("  Clearing dlt destination state files...")
        clear_dlt_destination_state()

    if clear_local_state:
        print("  Clearing dlt local state in Docker...")
        clear_dlt_local_state()

    postgres_schemas = []
    if clear_stg_temp:
        postgres_schemas.append("stg_temp")
    if clear_stg:
        postgres_schemas.append("stg")
    if clear_silver:
        postgres_schemas.append("silver")

    if postgres_schemas:
        print(f"  Dropping Postgres schemas: {', '.join(postgres_schemas)}...")
        clear_postgres_schemas(postgres_schemas)

    print("\n=== Reset complete ===\n")


if __name__ == "__main__":
    main()
