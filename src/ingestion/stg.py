import datetime
from pathlib import Path

import dlt
from dlt.destinations import postgres
from dlt.sources.filesystem import readers

from src.ingestion.config import SourceConfig, TableConfig


def get_parquet_dir(
    bronze_base_url: str,
    layer_subject_source: str,
    schema: str,
    table_name: str,
) -> str:
    # Biến đổi brz2stg_... thành src2brz_... để trỏ đúng vào thư mục nguồn
    src_folder = layer_subject_source.replace("brz2stg", "src2brz")
    path = f"{bronze_base_url}/{src_folder}/{schema}/{table_name}"
    print(f"[get_parquet_dir] Constructed path: {path}")
    return path


def get_latest_parquet_file(parquet_dir: str) -> Path | None:
    """Return the most recent parquet file in the directory based on modification time, or None."""
    print(f"[get_latest_parquet_file] Checking for directory: {parquet_dir}")
    base = Path(parquet_dir)
    if not base.exists():
        print(f"[get_latest_parquet_file] Directory DOES NOT EXIST: {parquet_dir}")
        return None
    print(f"[get_latest_parquet_file] Directory exists. Searching for parquet files...")
    
    files = list(base.glob("*.parquet"))
    if not files:
        print(f"[get_latest_parquet_file] No parquet files found in {parquet_dir}")
        return None
    
    # Sắp xếp file theo Modified Time giảm dần (file mới nhất theo giờ/phút/giây được đưa lên đầu)
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    latest = files[0]
    print(f"[get_latest_parquet_file] Found latest file: {latest}")
    return latest


def build_stg_pipeline(
    layer_subject_source: str,
    warehouse_credentials: str,
) -> dlt.Pipeline:
    dest = postgres(credentials=warehouse_credentials)
    dataset = f"stg_{layer_subject_source}"

    return dlt.pipeline(
        pipeline_name=f"stg_{layer_subject_source}",
        destination=dest,
        dataset_name=dataset,
    )


def _run_stg_load(pipeline, reader, table_name, layer_subject_source, credentials):
    """Run a stg load, resetting pipeline state on schema mismatch."""
    try:
        return pipeline.run(
            reader.with_name(table_name),
            write_disposition="replace",
        )
    except Exception as e:
        if "does not exist" in str(e):
            print(f"[stg__{layer_subject_source}] Schema mismatch for {table_name} — resetting pipeline state and retrying")
            pipeline.drop()
            pipeline = build_stg_pipeline(layer_subject_source, credentials)
            return pipeline.run(
                reader.with_name(table_name),
                write_disposition="replace",
            )
        raise


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


def run_stg_subject(
    source_name: str,
    source_schema: str,
    layer_subject_source: str,
    tables: list[TableConfig],
    bronze_base_url: str,
    warehouse_credentials: str,
) -> list[tuple]:
    """Load latest parquet files for a layer_subject_source into the warehouse.

    Returns list of (table_name, status, files_count, message) tuples.
    """
    pipeline = build_stg_pipeline(layer_subject_source, warehouse_credentials)

    if pipeline.has_pending_data:
        print(f"[stg__{layer_subject_source}] Dropping pending packages for {pipeline.pipeline_name}")
        pipeline.drop_pending_packages()

    results: list[tuple] = []
    for table_config in tables:
        parquet_dir = get_parquet_dir(
            bronze_base_url=bronze_base_url,
            layer_subject_source=layer_subject_source,
            schema=source_schema,
            table_name=table_config.name,
        )

        latest_file = get_latest_parquet_file(parquet_dir)

        if not latest_file:
            print(f"[stg__{layer_subject_source}] No parquet files for {table_config.name}, skipping")
            results.append((table_config.name, "skipped", 0, "no parquet files"))
            continue

        try:
            reader = readers(
                bucket_url=str(latest_file.parent),
                file_glob=latest_file.name,
            ).read_parquet()
            load_info = _run_stg_load(pipeline, reader, table_config.name, layer_subject_source, warehouse_credentials)
            print(f"[stg__{layer_subject_source}] Loaded {latest_file.name} → {table_config.name}: {load_info}")
            results.append((table_config.name, "loaded", 1, ""))
        except Exception as e:
            print(f"[stg__{layer_subject_source}] FAILED loading {table_config.name}: {e}")
            results.append((table_config.name, "failed", 0, str(e)))

    return results


def run_stg_ingestion(
    source_config: SourceConfig,
    bronze_base_url: str,
    warehouse_credentials: str,
) -> None:
    results: list[tuple] = []

    # Group tables by layer_subject_source so each group gets its own pipeline/schema
    subjects: dict[str, list] = {}
    for table_config in source_config.tables:
        subjects.setdefault(table_config.layer_subject_source, []).append(table_config)

    for layer_subject_source, tables in subjects.items():
        subject_results = run_stg_subject(
            source_name=source_config.name,
            source_schema=source_config.schema,
            layer_subject_source=layer_subject_source,
            tables=tables,
            bronze_base_url=bronze_base_url,
            warehouse_credentials=warehouse_credentials,
        )
        results.extend(subject_results)

    _print_stg_summary(source_config.name, results)
