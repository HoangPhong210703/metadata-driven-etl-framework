import pytest
from pathlib import Path
from src.ingestion.config import load_sources_config, SourceConfig, TableConfig


@pytest.fixture
def sample_config_path(tmp_path):
    config_content = """
sources:
  - name: test_db
    schema: public
    tables:
      - name: users
        load_strategy: full
        data_subject: test
      - name: events
        load_strategy: incremental
        data_subject: test
        cursor_column: created_at
        initial_value: "2024-01-01"
"""
    config_file = tmp_path / "sources.yaml"
    config_file.write_text(config_content)
    return config_file


def test_load_sources_config_returns_list(sample_config_path):
    sources = load_sources_config(sample_config_path)
    assert isinstance(sources, list)
    assert len(sources) == 1


def test_source_config_fields(sample_config_path):
    sources = load_sources_config(sample_config_path)
    source = sources[0]
    assert isinstance(source, SourceConfig)
    assert source.name == "test_db"
    assert source.schema == "public"
    assert len(source.tables) == 2


def test_table_config_full_strategy(sample_config_path):
    sources = load_sources_config(sample_config_path)
    table = sources[0].tables[0]
    assert isinstance(table, TableConfig)
    assert table.name == "users"
    assert table.load_strategy == "full"
    assert table.data_subject == "test"
    assert table.cursor_column is None
    assert table.initial_value is None


def test_table_config_incremental_strategy(sample_config_path):
    sources = load_sources_config(sample_config_path)
    table = sources[0].tables[1]
    assert table.name == "events"
    assert table.load_strategy == "incremental"
    assert table.data_subject == "test"
    assert table.cursor_column == "created_at"
    assert table.initial_value == "2024-01-01"


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_sources_config(Path("/nonexistent/sources.yaml"))


def test_load_config_missing_required_field(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("sources:\n  - name: test_db\n    tables: []")
    with pytest.raises(ValueError, match="schema"):
        load_sources_config(bad_config)


def test_invalid_load_strategy(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text(
        "sources:\n  - name: db\n    schema: public\n"
        "    tables:\n      - name: t\n        load_strategy: bogus\n        data_subject: x"
    )
    with pytest.raises(ValueError, match="Invalid load_strategy"):
        load_sources_config(bad_config)


def test_incremental_without_cursor_column(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text(
        "sources:\n  - name: db\n    schema: public\n"
        "    tables:\n      - name: t\n        load_strategy: incremental\n        data_subject: x"
    )
    with pytest.raises(ValueError, match="cursor_column"):
        load_sources_config(bad_config)


def test_table_config_single_primary_key(tmp_path):
    config = tmp_path / "sources.yaml"
    config.write_text("""
sources:
  - name: db
    schema: public
    tables:
      - name: t
        load_strategy: full
        data_subject: x
        primary_key: id
""")
    sources = load_sources_config(config)
    assert sources[0].tables[0].primary_key == ["id"]


def test_table_config_composite_primary_key(tmp_path):
    config = tmp_path / "sources.yaml"
    config.write_text("""
sources:
  - name: db
    schema: public
    tables:
      - name: t
        load_strategy: full
        data_subject: x
        primary_key:
          - org_id
          - user_id
""")
    sources = load_sources_config(config)
    assert sources[0].tables[0].primary_key == ["org_id", "user_id"]


def test_table_config_no_primary_key(sample_config_path):
    sources = load_sources_config(sample_config_path)
    table = sources[0].tables[0]
    assert table.primary_key is None
