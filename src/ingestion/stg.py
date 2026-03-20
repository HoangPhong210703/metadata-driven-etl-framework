import datetime
from pathlib import Path

import dlt
from dlt.destinations import postgres
from dlt.sources.filesystem import readers

from src.ingestion.bronze import extract_row_counts
from src.ingestion.config import SourceConfig, TableConfig


def get_parquet_dir(
    bronze_base_url: str,
    data_subject: str,
    source_name: str,
    schema: str,
    table_name: str,
) -> str:
    return f"{bronze_base_url}/{data_subject}/{source_name}/{schema}/{table_name}"


def get_latest_parquet_file(parquet_dir: str) -> Path | None:
    """Return the most recent parquet file in the directory, or None."""
    base = Path(parquet_dir)
    if not base.exists():
        return None
    files = sorted(base.glob("*.parquet"), reverse=True)
    return files[0] if files else None


def build_stg_pipeline(
    source_name: str,
    data_subject: str,
    warehouse_credentials: str,
) -> dlt.Pipeline:
    dest = postgres(credentials=warehouse_credentials)
    dataset = f"stg__{data_subject}__{source_name}"

    return dlt.pipeline(
        pipeline_name=f"stg_{source_name}_{data_subject}",
        destination=dest,
        dataset_name=dataset,
    )


def _run_stg_load(pipeline, reader, table_name, source_name, data_subject, credentials):
    """Run a stg load, resetting pipeline state on schema mismatch."""
    try:
        return pipeline.run(
            reader.with_name(table_name),
            write_disposition="replace",
        )
    except Exception as e:
        if "does not exist" in str(e):
            print(f"[stg__{data_subject}__{source_name}] Schema mismatch for {table_name} — resetting pipeline state and retrying")
            pipeline.drop()
            pipeline = build_stg_pipeline(source_name, data_subject, credentials)
            return pipeline.run(
                reader.with_name(table_name),
                write_disposition="replace",
            )
        raise


def _print_stg_summary(source_name: str, results: list[tuple]) -> None:
    print(f"\n{'=' * 55}")
    print(f"  Stg Ingestion Summary — {source_name}")
    print(f"{'=' * 55}")
    for table_name, status, files_count, message, row_count in results:
        icon = "OK" if status == "loaded" else ("FAIL" if status == "failed" else "SKIP")
        line = f"  [{icon}] {table_name:<30} "
        if status == "loaded":
            line += f"{files_count} file(s)"
            if row_count:
                line += f", {row_count} rows"
        elif message:
            line += message
        print(line)
    print(f"{'=' * 55}\n")


def run_stg_subject(
    source_name: str,
    source_schema: str,
    data_subject: str,
    tables: list[TableConfig],
    bronze_base_url: str,
    warehouse_credentials: str,
) -> list[tuple]:
    """Load latest parquet files for a data_subject into the warehouse.

    Returns list of (table_name, status, files_count, message, row_count) tuples.
    """
    pipeline = build_stg_pipeline(source_name, data_subject, warehouse_credentials)

    if pipeline.has_pending_data:
        print(f"[stg__{data_subject}__{source_name}] Dropping pending packages for {pipeline.pipeline_name}")
        pipeline.drop_pending_packages()

    results: list[tuple] = []
    for table_config in tables:
        parquet_dir = get_parquet_dir(
            bronze_base_url=bronze_base_url,
            data_subject=table_config.data_subject,
            source_name=source_name,
            schema=source_schema,
            table_name=table_config.name,
        )

        latest_file = get_latest_parquet_file(parquet_dir)

        if not latest_file:
            print(f"[stg__{data_subject}__{source_name}] No parquet files for {table_config.name}, skipping")
            results.append((table_config.name, "skipped", 0, "no parquet files", 0))
            continue

        try:
            reader = readers(
                bucket_url=str(latest_file.parent),
                file_glob=latest_file.name,
            ).read_parquet()
            load_info = _run_stg_load(pipeline, reader, table_config.name, source_name, data_subject, warehouse_credentials)
            counts = extract_row_counts(load_info)
            row_count = counts.get(table_config.name, 0)
            print(f"[stg__{data_subject}__{source_name}] Loaded {latest_file.name} → {table_config.name}: {load_info} ({row_count} rows)")
            results.append((table_config.name, "loaded", 1, "", row_count))
        except Exception as e:
            print(f"[stg__{data_subject}__{source_name}] FAILED loading {table_config.name}: {e}")
            results.append((table_config.name, "failed", 0, str(e), 0))

    return results


def run_stg_ingestion(
    source_config: SourceConfig,
    bronze_base_url: str,
    warehouse_credentials: str,
) -> None:
    results: list[tuple] = []

    # Group tables by data_subject so each group gets its own pipeline/schema
    subjects: dict[str, list] = {}
    for table_config in source_config.tables:
        subjects.setdefault(table_config.data_subject, []).append(table_config)

    for data_subject, tables in subjects.items():
        subject_results = run_stg_subject(
            source_name=source_config.name,
            source_schema=source_config.schema,
            data_subject=data_subject,
            tables=tables,
            bronze_base_url=bronze_base_url,
            warehouse_credentials=warehouse_credentials,
        )
        results.extend(subject_results)

    _print_stg_summary(source_config.name, results)
