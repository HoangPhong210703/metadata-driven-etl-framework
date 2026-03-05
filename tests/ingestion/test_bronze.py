import datetime

import pytest
from src.ingestion.config import SourceConfig, TableConfig
from src.ingestion.bronze import build_pipeline, build_layout, build_bucket_url, cleanup_todays_parquet


@pytest.fixture
def source_config():
    return SourceConfig(
        name="test_pg",
        data_subject="crm",
        schema="public",
        tables=[
            TableConfig(name="customers", load_strategy="full"),
            TableConfig(
                name="orders",
                load_strategy="incremental",
                cursor_column="updated_at",
                initial_value="2024-01-01",
            ),
        ],
    )


def test_build_layout():
    layout = build_layout()
    assert "{table_name}" in layout
    assert "{DD}" in layout
    assert "{MM}" in layout
    assert "{YYYY}" in layout
    assert layout.endswith(".{ext}")


def test_build_bucket_url(source_config):
    url = build_bucket_url("data/bronze", source_config)
    assert url == "data/bronze/crm/test_pg"


def test_build_pipeline_returns_dlt_pipeline(source_config):
    pipeline = build_pipeline(source_config, bucket_url="/tmp/test_bronze")
    assert pipeline.pipeline_name == "bronze_test_pg"
    assert pipeline.destination.destination_name == "filesystem"


def test_cleanup_todays_parquet_deletes_existing_file(tmp_path, source_config):
    """If today's parquet exists, cleanup should delete it."""
    today = datetime.date.today().strftime("%d-%m-%Y")
    # Create the directory structure: bucket_url/data_subject/source_name/schema/table/DD-MM-YYYY.parquet
    parquet_dir = tmp_path / "crm" / "test_pg" / "public" / "customers"
    parquet_dir.mkdir(parents=True)
    parquet_file = parquet_dir / f"{today}.parquet"
    parquet_file.write_bytes(b"fake parquet data")

    cleanup_todays_parquet(str(tmp_path), source_config)

    assert not parquet_file.exists()


def test_cleanup_todays_parquet_preserves_other_dates(tmp_path, source_config):
    """Cleanup should only delete today's file, not other dates."""
    today = datetime.date.today().strftime("%d-%m-%Y")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%d-%m-%Y")

    parquet_dir = tmp_path / "crm" / "test_pg" / "public" / "customers"
    parquet_dir.mkdir(parents=True)
    today_file = parquet_dir / f"{today}.parquet"
    yesterday_file = parquet_dir / f"{yesterday}.parquet"
    today_file.write_bytes(b"today data")
    yesterday_file.write_bytes(b"yesterday data")

    cleanup_todays_parquet(str(tmp_path), source_config)

    assert not today_file.exists()
    assert yesterday_file.exists()


def test_cleanup_todays_parquet_no_file_is_noop(tmp_path, source_config):
    """Cleanup should not raise if today's file doesn't exist."""
    cleanup_todays_parquet(str(tmp_path), source_config)
