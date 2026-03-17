import datetime
from pathlib import Path

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database
from sqlalchemy import create_engine, text

from src.ingestion.config import SourceConfig, load_source_configs


def _parse_date(value: str) -> datetime.datetime:
    """Parse a date string in ISO format (2024-01-01) or US format (1/1/2024)."""
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return datetime.datetime.strptime(value, "%m/%d/%Y")


def test_source_connection(credentials: str, schema: str) -> None:
    """Test that the source database is reachable. Raises on failure."""
    engine = create_engine(credentials)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    engine.dispose()
    print(f"[source_connection] Connection to schema '{schema}' OK")


def build_layout() -> str:
    return "{table_name}/{DD}-{MM}-{YYYY}.{ext}"


def build_bucket_url(base_url: str, layer_subject_source: str) -> str:
    return f"{base_url}/{layer_subject_source}"


def build_pipeline(source_config: SourceConfig, bucket_url: str, layer_subject_source: str) -> dlt.Pipeline:
    dest = filesystem(
        bucket_url=build_bucket_url(bucket_url, layer_subject_source),
        layout=build_layout(),
    )

    return dlt.pipeline(
        pipeline_name=f"bronze_{layer_subject_source}",
        destination=dest,
        dataset_name=source_config.schema,
    )


def _is_first_run(pipeline: dlt.Pipeline) -> bool:
    """Check if this pipeline has been run before by looking for existing state."""
    try:
        return pipeline.state.get("default_schema_name") is None
    except Exception:
        return True


def rotate_todays_parquet(bucket_url: str, source_config: SourceConfig) -> None:
    """If today's parquet exists, rename it with the next available suffix before the new run."""
    today = datetime.date.today().strftime("%d-%m-%Y")

    for table_config in source_config.tables:
        base = Path(build_bucket_url(bucket_url, table_config.layer_subject_source))
        parquet_file = base / source_config.schema / table_config.name / f"{today}.parquet"
        if parquet_file.exists():
            suffix = 1
            while True:
                rotated = parquet_file.parent / f"{today}({suffix}).parquet"
                if not rotated.exists():
                    parquet_file.rename(rotated)
                    print(f"[{source_config.name}] Rotated {parquet_file.name} → {rotated.name}")
                    break
                suffix += 1


def _group_tables_by_layer_subject_source(source_config: SourceConfig) -> dict[str, list]:
    from collections import defaultdict

    groups = defaultdict(list)
    for table_config in source_config.tables:
        groups[table_config.layer_subject_source].append(table_config)
    return dict(groups)


def _print_bronze_summary(source_name: str, results: list[tuple]) -> None:
    print(f"\n{'=' * 55}")
    print(f"  Bronze Ingestion Summary — {source_name}")
    print(f"{'=' * 55}")
    for table_name, data_subject, status, message in results:
        icon = "OK" if status == "loaded" else ("FAIL" if status == "failed" else "SKIP")
        line = f"  [{icon}] {table_name:<30} ({data_subject})"
        if message:
            line += f"  {message}"
        print(line)
    print(f"{'=' * 55}\n")


def run_source_ingestion(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
) -> None:
    results: list[tuple] = []

    groups = _group_tables_by_layer_subject_source(source_config)

    for layer_subject_source, table_configs in groups.items():
        pipeline = build_pipeline(source_config, bucket_url, layer_subject_source)
        first_run = _is_first_run(pipeline)

        if first_run:
            print(f"[{source_config.name}/{layer_subject_source}] First run detected — performing full load")
        else:
            print(f"[{source_config.name}/{layer_subject_source}] Subsequent run — using configured load strategies")

        table_names = [t.name for t in table_configs]
        source = sql_database(
            credentials=credentials,
            schema=source_config.schema,
            table_names=table_names,
            backend="pyarrow",
        )

        if not first_run:
            for table_config in table_configs:
                if table_config.load_strategy == "incremental" and table_config.cursor_column:
                    resource = source.resources[table_config.name]
                    initial_value = table_config.initial_value
                    if initial_value:
                        initial_value = _parse_date(initial_value)
                    resource.apply_hints(
                        incremental=dlt.sources.incremental(
                            table_config.cursor_column,
                            initial_value=initial_value,
                        ),
                    )

        try:
            load_info = pipeline.run(source, write_disposition="append", loader_file_format="parquet")
            print(f"[{source_config.name}/{layer_subject_source}] Load complete: {load_info}")
            for table_config in table_configs:
                results.append((table_config.name, table_config.data_subject, "loaded", ""))
        except Exception as e:
            print(f"[{source_config.name}/{layer_subject_source}] Load FAILED: {e}")
            for table_config in table_configs:
                results.append((table_config.name, table_config.data_subject, "failed", str(e)))

    _print_bronze_summary(source_config.name, results)


def run_data_subject_ingestion(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
    data_subject: str,
) -> None:
    """Ingest all tables for a single data_subject."""
    table_configs = [t for t in source_config.tables if t.data_subject == data_subject]
    layer_subject_source = table_configs[0].layer_subject_source if table_configs else data_subject
    pipeline = build_pipeline(source_config, bucket_url, layer_subject_source)
    first_run = _is_first_run(pipeline)

    if first_run:
        print(f"[{source_config.name}/{layer_subject_source}] First run detected — performing full load")
    else:
        print(f"[{source_config.name}/{layer_subject_source}] Subsequent run — using configured load strategies")

    table_names = [t.name for t in table_configs]
    source = sql_database(
        credentials=credentials,
        schema=source_config.schema,
        table_names=table_names,
        backend="pyarrow",
    )

    if not first_run:
        for table_config in table_configs:
            if table_config.load_strategy == "incremental" and table_config.cursor_column:
                resource = source.resources[table_config.name]
                initial_value = table_config.initial_value
                if initial_value:
                    initial_value = _parse_date(initial_value)
                resource.apply_hints(
                    incremental=dlt.sources.incremental(
                        table_config.cursor_column,
                        initial_value=initial_value,
                    ),
                )

    load_info = pipeline.run(source, write_disposition="append", loader_file_format="parquet")
    print(f"[{source_config.name}/{layer_subject_source}] Load complete: {load_info}")


def extract_tables(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
    data_subject: str,
) -> None:
    """Fetch data from RDBMS and normalize (extract + normalize step of dlt)."""
    table_configs = [t for t in source_config.tables if t.data_subject == data_subject]
    layer_subject_source = table_configs[0].layer_subject_source if table_configs else data_subject
    pipeline = build_pipeline(source_config, bucket_url, layer_subject_source)
    first_run = _is_first_run(pipeline)

    if first_run:
        print(f"[{source_config.name}/{layer_subject_source}] First run — full load")
    else:
        print(f"[{source_config.name}/{layer_subject_source}] Subsequent run — configured strategies")

    table_names = [t.name for t in table_configs]
    source = sql_database(
        credentials=credentials,
        schema=source_config.schema,
        table_names=table_names,
        backend="pyarrow",
    )

    if not first_run:
        for table_config in table_configs:
            if table_config.load_strategy == "incremental" and table_config.cursor_column:
                resource = source.resources[table_config.name]
                initial_value = table_config.initial_value
                if initial_value:
                    initial_value = _parse_date(initial_value)
                resource.apply_hints(
                    incremental=dlt.sources.incremental(
                        table_config.cursor_column,
                        initial_value=initial_value,
                    ),
                )

    pipeline.extract(source, write_disposition="append", loader_file_format="parquet")
    pipeline.normalize()
    print(f"[{source_config.name}/{layer_subject_source}] Extract + normalize complete")


def load_to_parquet(
    source_config: SourceConfig,
    bucket_url: str,
    data_subject: str,
) -> None:
    """Write normalized data to parquet files (load step of dlt)."""
    table_configs = [t for t in source_config.tables if t.data_subject == data_subject]
    layer_subject_source = table_configs[0].layer_subject_source if table_configs else data_subject
    pipeline = build_pipeline(source_config, bucket_url, layer_subject_source)
    load_info = pipeline.load()
    print(f"[{source_config.name}/{layer_subject_source}] Write parquet complete: {load_info}")


def run_all_sources(config_path: Path, bucket_url: str, secrets: dict[str, str]) -> None:
    sources = load_source_configs(config_path)

    for source_config in sources:
        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials found")
            continue

        run_source_ingestion(source_config, bucket_url, credentials)
