import pytest
from pathlib import Path
from src.config.config import (
    load_csv_config,
    load_source_configs,
    csv_to_source_configs,
    get_active_tables,
    get_data_subjects,
    get_active_data_subjects,
    CsvTableConfig,
    SourceConfig,
    TableConfig,
)

CSV_HEADER = "id,table_name,source_name,source_schema,data_subject,load_strategy,cursor_column,initial_value,primary_key,load_sequence,table_load_active"


@pytest.fixture
def sample_csv(tmp_path):
    content = f"""{CSV_HEADER}
1,users,test_db,public,auth,full,,,id,10,TRUE
2,events,test_db,public,analytics,incremental,created_at,2024-01-01,id,20,TRUE
3,logs,test_db,public,analytics,append,,,id,30,FALSE
"""
    csv_file = tmp_path / "config.csv"
    csv_file.write_text(content)
    return csv_file


def test_load_csv_config_returns_list(sample_csv):
    configs = load_csv_config(sample_csv)
    assert isinstance(configs, list)
    assert len(configs) == 3
    assert all(isinstance(c, CsvTableConfig) for c in configs)


def test_csv_table_config_fields(sample_csv):
    configs = load_csv_config(sample_csv)
    c = configs[0]
    assert c.id == 1
    assert c.table_name == "users"
    assert c.source_name == "test_db"
    assert c.source_schema == "public"
    assert c.data_subject == "auth"
    assert c.load_strategy == "full"
    assert c.cursor_column == ""
    assert c.primary_key == "id"
    assert c.load_sequence == 10
    assert c.table_load_active is True


def test_csv_incremental_fields(sample_csv):
    configs = load_csv_config(sample_csv)
    c = configs[1]
    assert c.table_name == "events"
    assert c.load_strategy == "incremental"
    assert c.cursor_column == "created_at"
    assert c.initial_value == "2024-01-01"


def test_csv_inactive_table(sample_csv):
    configs = load_csv_config(sample_csv)
    c = configs[2]
    assert c.table_name == "logs"
    assert c.table_load_active is False


def test_load_csv_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_csv_config(Path("/nonexistent/config.csv"))


def test_get_active_tables(sample_csv):
    configs = load_csv_config(sample_csv)
    active = get_active_tables(configs)
    assert len(active) == 2
    assert active[0].table_name == "users"
    assert active[1].table_name == "events"


def test_get_active_tables_sorted_by_load_sequence(sample_csv):
    configs = load_csv_config(sample_csv)
    active = get_active_tables(configs)
    sequences = [c.load_sequence for c in active]
    assert sequences == sorted(sequences)


def test_get_data_subjects(sample_csv):
    configs = load_csv_config(sample_csv)
    subjects = get_data_subjects(configs)
    assert subjects == {"auth", "analytics"}


def test_get_active_data_subjects(sample_csv):
    configs = load_csv_config(sample_csv)
    subjects = get_active_data_subjects(configs)
    assert subjects == {"auth", "analytics"}


def test_csv_to_source_configs(sample_csv):
    configs = load_csv_config(sample_csv)
    sources = csv_to_source_configs(configs)
    assert len(sources) == 1
    assert isinstance(sources[0], SourceConfig)
    assert sources[0].name == "test_db"
    assert sources[0].schema == "public"
    assert len(sources[0].tables) == 3


def test_csv_to_source_configs_table_fields(sample_csv):
    configs = load_csv_config(sample_csv)
    sources = csv_to_source_configs(configs)
    tables = sources[0].tables

    assert isinstance(tables[0], TableConfig)
    assert tables[0].name == "users"
    assert tables[0].load_strategy == "full"
    assert tables[0].data_subject == "auth"
    assert tables[0].cursor_column is None
    assert tables[0].primary_key == ["id"]

    assert tables[1].name == "events"
    assert tables[1].load_strategy == "incremental"
    assert tables[1].cursor_column == "created_at"
    assert tables[1].initial_value == "2024-01-01"


def test_csv_to_source_configs_multiple_sources(tmp_path):
    content = f"""{CSV_HEADER}
1,users,db1,public,auth,full,,,id,10,TRUE
2,orders,db2,appdb,sales,incremental,modified,,id,10,TRUE
"""
    csv_file = tmp_path / "config.csv"
    csv_file.write_text(content)

    configs = load_csv_config(csv_file)
    sources = csv_to_source_configs(configs)
    assert len(sources) == 2
    names = {s.name for s in sources}
    assert names == {"db1", "db2"}

    db2 = next(s for s in sources if s.name == "db2")
    assert db2.schema == "appdb"


def test_load_source_configs(sample_csv):
    """End-to-end: load CSV → active tables only → SourceConfig objects."""
    sources = load_source_configs(sample_csv)
    assert len(sources) == 1
    # Only active tables (2 of 3)
    assert len(sources[0].tables) == 2
    table_names = [t.name for t in sources[0].tables]
    assert "logs" not in table_names


def test_csv_no_primary_key(tmp_path):
    content = f"""{CSV_HEADER}
1,t,db,public,x,full,,,,10,TRUE
"""
    csv_file = tmp_path / "config.csv"
    csv_file.write_text(content)

    configs = load_csv_config(csv_file)
    sources = csv_to_source_configs(configs)
    assert sources[0].tables[0].primary_key is None
