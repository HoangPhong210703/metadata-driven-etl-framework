import argparse
import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from src.ingestion.config import load_source_configs
from src.ingestion.stg import run_stg_ingestion


def load_warehouse_credentials(secrets_path: Path) -> str:
    import tomllib

    if not secrets_path.exists():
        raise FileNotFoundError(
            f"Secrets file not found: {secrets_path}. "
            f"Copy .dlt/secrets.toml.example to .dlt/secrets.toml and fill in credentials."
        )

    with open(secrets_path, "rb") as f:
        raw = tomllib.load(f)

    warehouse = raw.get("destinations", {}).get("warehouse", {})
    credentials = warehouse.get("credentials")
    if not credentials:
        raise ValueError("No warehouse credentials found in secrets at [destinations.warehouse]")

    return credentials


def set_dbt_env_vars(credentials: str) -> None:
    """Parse connection string and set env vars for dbt profiles.yml."""
    parsed = urlparse(credentials)
    os.environ["WAREHOUSE_HOST"] = parsed.hostname or "localhost"
    os.environ["WAREHOUSE_PORT"] = str(parsed.port or 5432)
    os.environ["WAREHOUSE_USER"] = parsed.username or ""
    os.environ["WAREHOUSE_PASSWORD"] = parsed.password or ""
    os.environ["WAREHOUSE_DB"] = (parsed.path or "").lstrip("/")


def run_dbt(dbt_project_dir: Path) -> None:
    """Run dbt models for the stg layer."""
    profiles_dir = dbt_project_dir
    result = subprocess.run(
        ["dbt", "run", "--select", "stg", "--profiles-dir", str(profiles_dir)],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")


def main():
    parser = argparse.ArgumentParser(description="Staging layer ingestion")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/src2brz_config.csv"),
        help="Path to CSV config file",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".dlt/secrets.toml"),
        help="Path to secrets.toml with credentials",
    )
    parser.add_argument(
        "--bronze-url",
        type=str,
        default="data/bronze",
        help="Base directory of bronze parquet files",
    )
    parser.add_argument(
        "--dbt-dir",
        type=Path,
        default=Path("dbt"),
        help="Path to dbt project directory",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source by name (default: run all)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Number of days to retain in stg_temp (default: 7)",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt run (only load parquet into stg_temp)",
    )
    args = parser.parse_args()

    sources = load_source_configs(args.config)
    warehouse_credentials = load_warehouse_credentials(args.secrets)

    # Step 1: dlt — load recent parquet → stg_temp
    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue
        print(f"[stg_{source_config.name}] Loading parquet into stg_temp...")
        run_stg_ingestion(source_config, args.bronze_url, warehouse_credentials)

    # Step 2: dbt — build stg newest tables
    if not args.skip_dbt:
        print("[stg] Running dbt models...")
        set_dbt_env_vars(warehouse_credentials)
        run_dbt(args.dbt_dir)


if __name__ == "__main__":
    main()
