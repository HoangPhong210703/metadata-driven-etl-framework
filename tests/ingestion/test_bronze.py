import datetime

import pytest
from src.ingestion.config import SourceConfig, TableConfig
from src.ingestion.bronze import build_pipeline, build_layout, build_bucket_url, rotate_todays_parquet


@pytest.fixture
def source_config():
    return SourceConfig(
        name="test_pg",
        schema="public",
        tables=[
            TableConfig(name="customers", load_strategy="full", data_subject="crm"),
            TableConfig(
                name="orders",
                load_strategy="incremental",
                data_subject="crm",
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
    url = build_bucket_url("data/bronze", source_config, "crm")
    assert url == "data/bronze/crm/test_pg"


def test_build_pipeline_returns_dlt_pipeline(source_config):
    pipeline = build_pipeline(source_config, bucket_url="/tmp/test_bronze", data_subject="crm")
    assert pipeline.pipeline_name == "bronze_test_pg_crm"
    assert pipeline.destination.destination_name == "filesystem"


def test_rotate_todays_parquet_no_file_is_noop(tmp_path, source_config):
    """If no file exists today, rotate should do nothing."""
    rotate_todays_parquet(str(tmp_path), source_config)


def test_rotate_todays_parquet_renames_existing_file(tmp_path, source_config):
    """If today's parquet exists, it should be renamed with suffix (1)."""
    today = datetime.date.today().strftime("%d-%m-%Y")
    parquet_dir = tmp_path / "crm" / "test_pg" / "public" / "customers"
    parquet_dir.mkdir(parents=True)
    parquet_file = parquet_dir / f"{today}.parquet"
    parquet_file.write_bytes(b"first load")

    rotate_todays_parquet(str(tmp_path), source_config)

    assert not parquet_file.exists()
    assert (parquet_dir / f"{today}(1).parquet").exists()


def test_rotate_todays_parquet_increments_suffix(tmp_path, source_config):
    """If today's file and (1) both exist, rename to (2)."""
    today = datetime.date.today().strftime("%d-%m-%Y")
    parquet_dir = tmp_path / "crm" / "test_pg" / "public" / "customers"
    parquet_dir.mkdir(parents=True)
    (parquet_dir / f"{today}.parquet").write_bytes(b"second load")
    (parquet_dir / f"{today}(1).parquet").write_bytes(b"first load")

    rotate_todays_parquet(str(tmp_path), source_config)

    assert not (parquet_dir / f"{today}.parquet").exists()
    assert (parquet_dir / f"{today}(1).parquet").exists()
    assert (parquet_dir / f"{today}(2).parquet").exists()


def test_rotate_todays_parquet_preserves_other_dates(tmp_path, source_config):
    """Rotate should not touch files from other dates."""
    today = datetime.date.today().strftime("%d-%m-%Y")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%d-%m-%Y")

    parquet_dir = tmp_path / "crm" / "test_pg" / "public" / "customers"
    parquet_dir.mkdir(parents=True)
    (parquet_dir / f"{today}.parquet").write_bytes(b"today data")
    yesterday_file = parquet_dir / f"{yesterday}.parquet"
    yesterday_file.write_bytes(b"yesterday data")

    rotate_todays_parquet(str(tmp_path), source_config)

    assert yesterday_file.exists()
