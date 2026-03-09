import datetime
from pathlib import Path

import dlt
from dlt.destinations import postgres
from dlt.sources.filesystem import readers

from src.ingestion.config import SourceConfig, load_sources_config


def get_parquet_dir(
    bronze_base_url: str,
    data_subject: str,
    source_name: str,
    schema: str,
    table_name: str,
) -> str:
    return f"{bronze_base_url}/{data_subject}/{source_name}/{schema}/{table_name}"


def get_recent_parquet_files(parquet_dir: str, retention_days: int = 7) -> list[Path]:
    """Return parquet files from last N days, sorted oldest first."""
    today = datetime.date.today()
    recent_dates = {
        (today - datetime.timedelta(days=i)).strftime("%d-%m-%Y")
        for i in range(retention_days)
    }
    base = Path(parquet_dir)
    if not base.exists():
        return []
    return sorted(
        f for f in base.glob("*.parquet")
        if any(f.name.startswith(d) for d in recent_dates)
    )


def build_stg_pipeline(
    source_config: SourceConfig,
    warehouse_credentials: str,
) -> dlt.Pipeline:
    dest = postgres(credentials=warehouse_credentials)

    return dlt.pipeline(
        pipeline_name=f"stg_{source_config.name}",
        destination=dest,
        dataset_name="stg_temp",
    )


def _print_stg_summary(source_name: str, results: list[tuple]) -> None:
    print(f"\n{'=' * 55}")
    print(f"  Stg Ingestion Summary — {source_name}")
    print(f"{'=' * 55}")
    for table_name, status, files_count, message in results:
        icon = "OK" if status == "loaded" else ("FAIL" if status == "failed" else "SKIP")
        line = f"  [{icon}] {table_name:<30} "
        if status == "loaded":
            line += f"{files_count} file(s)"
        elif message:
            line += message
        print(line)
    print(f"{'=' * 55}\n")


def run_stg_ingestion(
    source_config: SourceConfig,
    bronze_base_url: str,
    warehouse_credentials: str,
    retention_days: int = 7,
) -> None:
    results: list[tuple] = []
    pipeline = build_stg_pipeline(source_config, warehouse_credentials)

    for table_config in source_config.tables:
        parquet_dir = get_parquet_dir(
            bronze_base_url=bronze_base_url,
            data_subject=table_config.data_subject,
            source_name=source_config.name,
            schema=source_config.schema,
            table_name=table_config.name,
        )

        recent_files = get_recent_parquet_files(parquet_dir, retention_days)

        if not recent_files:
            print(f"[stg_{source_config.name}] No recent parquet files for {table_config.name}, skipping")
            results.append((table_config.name, "skipped", 0, "no recent parquet files"))
            continue

        try:
            for i, parquet_file in enumerate(recent_files):
                disposition = "replace" if i == 0 else "append"
                reader = readers(
                    bucket_url=str(parquet_file.parent),
                    file_glob=parquet_file.name,
                ).read_parquet()
                stg_temp_name = f"temp_{table_config.data_subject}_{source_config.name}_{table_config.name}"
                load_info = pipeline.run(
                    reader.with_name(stg_temp_name),
                    write_disposition=disposition,
                )
                print(f"[stg_{source_config.name}] Loaded {parquet_file.name} → {table_config.name} ({disposition}): {load_info}")
            results.append((table_config.name, "loaded", len(recent_files), ""))
        except Exception as e:
            print(f"[stg_{source_config.name}] FAILED loading {table_config.name}: {e}")
            results.append((table_config.name, "failed", 0, str(e)))

    _print_stg_summary(source_config.name, results)


def run_all_stg_sources(
    config_path: Path,
    bronze_base_url: str,
    warehouse_credentials: str,
    retention_days: int = 7,
) -> None:
    sources = load_sources_config(config_path)

    for source_config in sources:
        print(f"[stg_{source_config.name}] Starting stg ingestion...")
        run_stg_ingestion(source_config, bronze_base_url, warehouse_credentials, retention_days)
