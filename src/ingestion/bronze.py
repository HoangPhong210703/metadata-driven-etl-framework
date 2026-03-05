import datetime
from pathlib import Path

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database

from src.ingestion.config import SourceConfig, load_sources_config


def build_layout() -> str:
    return "{table_name}/{DD}-{MM}-{YYYY}.{ext}"


def build_bucket_url(base_url: str, source_config: SourceConfig) -> str:
    return f"{base_url}/{source_config.data_subject}/{source_config.name}"


def build_pipeline(source_config: SourceConfig, bucket_url: str) -> dlt.Pipeline:
    dest = filesystem(
        bucket_url=build_bucket_url(bucket_url, source_config),
        layout=build_layout(),
    )

    return dlt.pipeline(
        pipeline_name=f"bronze_{source_config.name}",
        destination=dest,
        dataset_name=source_config.schema,
    )


def _is_first_run(pipeline: dlt.Pipeline) -> bool:
    """Check if this pipeline has been run before by looking for existing state."""
    try:
        return pipeline.state.get("default_schema_name") is None
    except Exception:
        return True


def cleanup_todays_parquet(bucket_url: str, source_config: SourceConfig) -> None:
    """Delete today's parquet files so only the latest load per day is kept."""
    today = datetime.date.today().strftime("%d-%m-%Y")
    base = Path(build_bucket_url(bucket_url, source_config))

    for table_config in source_config.tables:
        parquet_file = base / source_config.schema / table_config.name / f"{today}.parquet"
        if parquet_file.exists():
            parquet_file.unlink()
            print(f"[{source_config.name}] Removed existing {parquet_file}")


def run_source_ingestion(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
) -> None:
    cleanup_todays_parquet(bucket_url, source_config)

    pipeline = build_pipeline(source_config, bucket_url)
    first_run = _is_first_run(pipeline)

    if first_run:
        print(f"[{source_config.name}] First run detected — performing full load")
    else:
        print(f"[{source_config.name}] Subsequent run — using configured load strategies")

    table_names = [t.name for t in source_config.tables]
    source = sql_database(
        credentials=credentials,
        schema=source_config.schema,
        table_names=table_names,
        backend="pyarrow",
    )

    # On subsequent runs, apply incremental hints for tables configured as incremental
    if not first_run:
        for table_config in source_config.tables:
            if table_config.load_strategy == "incremental" and table_config.cursor_column:
                resource = source.resources[table_config.name]
                initial_value = table_config.initial_value
                if initial_value:
                    initial_value = datetime.datetime.fromisoformat(initial_value)
                resource.apply_hints(
                    incremental=dlt.sources.incremental(
                        table_config.cursor_column,
                        initial_value=initial_value,
                    ),
                )

    load_info = pipeline.run(source, write_disposition="append", loader_file_format="parquet")
    print(f"[{source_config.name}] Load complete: {load_info}")


def run_all_sources(config_path: Path, bucket_url: str, secrets: dict[str, str]) -> None:
    sources = load_sources_config(config_path)

    for source_config in sources:
        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials found")
            continue

        run_source_ingestion(source_config, bucket_url, credentials)
