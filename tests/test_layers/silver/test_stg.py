import datetime
import pytest
from pathlib import Path
from src.config.config import SourceConfig, TableConfig
from src.layer.silver.stg import (
    get_parquet_dir,
    get_recent_parquet_files,
    build_stg_pipeline,
)


@pytest.fixture
def source_config():
    return SourceConfig(
        name="postgres_crm",
        schema="public",
        tables=[
            TableConfig(name="project_task", load_strategy="full", data_subject="crm", primary_key=["id"]),
        ],
    )


def test_get_parquet_dir():
    path = get_parquet_dir(
        bronze_base_url="data/bronze",
        data_subject="crm",
        source_name="postgres_crm",
        schema="public",
        table_name="project_task",
    )
    assert path == "data/bronze/crm/postgres_crm/public/project_task"


def test_build_stg_pipeline(source_config):
    pipeline = build_stg_pipeline(
        source_config=source_config,
        warehouse_credentials="postgresql://user:pass@localhost:5432/test_db",
    )
    assert pipeline.pipeline_name == "stg_postgres_crm"
    assert pipeline.destination.destination_name == "postgres"
    assert pipeline.dataset_name == "stg_temp"


def test_get_recent_parquet_files_returns_files_within_retention(tmp_path):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    old = today - datetime.timedelta(days=8)

    (tmp_path / f"{today.strftime('%d-%m-%Y')}.parquet").write_bytes(b"today")
    (tmp_path / f"{yesterday.strftime('%d-%m-%Y')}.parquet").write_bytes(b"yesterday")
    (tmp_path / f"{old.strftime('%d-%m-%Y')}.parquet").write_bytes(b"old")

    result = get_recent_parquet_files(str(tmp_path), retention_days=7)
    names = [f.name for f in result]

    assert f"{today.strftime('%d-%m-%Y')}.parquet" in names
    assert f"{yesterday.strftime('%d-%m-%Y')}.parquet" in names
    assert f"{old.strftime('%d-%m-%Y')}.parquet" not in names


def test_get_recent_parquet_files_includes_rotated_files(tmp_path):
    today = datetime.date.today().strftime("%d-%m-%Y")
    (tmp_path / f"{today}.parquet").write_bytes(b"latest")
    (tmp_path / f"{today}(1).parquet").write_bytes(b"first load")

    result = get_recent_parquet_files(str(tmp_path), retention_days=7)
    names = [f.name for f in result]

    assert f"{today}.parquet" in names
    assert f"{today}(1).parquet" in names


def test_get_recent_parquet_files_empty_dir(tmp_path):
    result = get_recent_parquet_files(str(tmp_path), retention_days=7)
    assert result == []


def test_get_recent_parquet_files_missing_dir():
    result = get_recent_parquet_files("/nonexistent/path", retention_days=7)
    assert result == []
