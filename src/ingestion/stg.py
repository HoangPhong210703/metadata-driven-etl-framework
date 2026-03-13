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
    dataset = f"stg__{source_name}__{data_subject}"

    return dlt.pipeline(
        pipeline_name=f"stg_{source_name}_{data_subject}",
        destination=dest,
        dataset_name=dataset,
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
) -> None:
    results: list[tuple] = []

    # Group tables by data_subject so each group gets its own pipeline/schema
    subjects: dict[str, list] = {}
    for table_config in source_config.tables:
        subjects.setdefault(table_config.data_subject, []).append(table_config)

    for data_subject, tables in subjects.items():
        pipeline = build_stg_pipeline(source_config.name, data_subject, warehouse_credentials)

        for table_config in tables:
            parquet_dir = get_parquet_dir(
                bronze_base_url=bronze_base_url,
                data_subject=table_config.data_subject,
                source_name=source_config.name,
                schema=source_config.schema,
                table_name=table_config.name,
            )

            latest_file = get_latest_parquet_file(parquet_dir)

            if not latest_file:
                print(f"[stg__{source_config.name}__{data_subject}] No parquet files for {table_config.name}, skipping")
                results.append((table_config.name, "skipped", 0, "no parquet files"))
                continue

            try:
                reader = readers(
                    bucket_url=str(latest_file.parent),
                    file_glob=latest_file.name,
                ).read_parquet()
                load_info = pipeline.run(
                    reader.with_name(table_config.name),
                    write_disposition="replace",
                )
                print(f"[stg__{source_config.name}__{data_subject}] Loaded {latest_file.name} → {table_config.name}: {load_info}")
                results.append((table_config.name, "loaded", 1, ""))
            except Exception as e:
                print(f"[stg__{source_config.name}__{data_subject}] FAILED loading {table_config.name}: {e}")
                results.append((table_config.name, "failed", 0, str(e)))

    _print_stg_summary(source_config.name, results)


def run_all_stg_sources(
    config_path: Path,
    bronze_base_url: str,
    warehouse_credentials: str,
) -> None:
    sources = load_sources_config(config_path)

    for source_config in sources:
        print(f"[stg_{source_config.name}] Starting stg ingestion...")
        run_stg_ingestion(source_config, bronze_base_url, warehouse_credentials)
