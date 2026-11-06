import copy
from pathlib import Path

import pytest

from lakehouse_framework import ConfigError, load_config, parse_config

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "pipeline.yaml"

BASE = {
    "tables": [
        {
            "name": "t",
            "source": {"path": "data/{batch}/t.csv"},
            "load_type": "scd2",
            "keys": ["id"],
            "watermark_column": "ts",
            "schema": {"id": "int", "a": "string", "b": "decimal(10,2)", "ts": "timestamp"},
        }
    ]
}


def cfg(**changes):
    doc = copy.deepcopy(BASE)
    doc["tables"][0].update(changes)
    return doc


def test_sample_config_loads():
    config = load_config(CONFIG)
    assert [t.name for t in config.tables] == ["customers", "products", "orders"]
    assert [t.load_type for t in config.tables] == ["scd2", "scd1", "insert_only"]
    customers = config.table("customers")
    assert customers.required_columns == ["customer_id", "updated_at"]
    assert customers.source_options == {"header": "true"}
    assert [r.name for r in customers.quality] == ["email_present", "email_format"]


def test_scd2_tracks_all_non_key_columns_by_default():
    table = parse_config(cfg()).tables[0]
    assert table.track_columns == ["a", "b"]


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"load_type": "merge"}, "load_type"),
        ({"keys": []}, "keys"),
        ({"keys": ["missing"]}, "key column"),
        ({"watermark_column": "nope"}, "watermark"),
        ({"schema": {"id": "int", "ts": "varchar(10)"}}, "unsupported type"),
        ({"quality": [{"name": "r", "rule": "a > 0"}, {"name": "r", "rule": "b > 0"}]}, "duplicate quality rule"),
    ],
)
def test_invalid_tables_are_rejected(changes, message):
    with pytest.raises(ConfigError, match=message):
        parse_config(cfg(**changes))


def test_duplicate_table_names_are_rejected():
    doc = copy.deepcopy(BASE)
    doc["tables"].append(copy.deepcopy(doc["tables"][0]))
    with pytest.raises(ConfigError, match="duplicate table names"):
        parse_config(doc)


def test_source_paths_resolve_batch_and_root():
    config = load_config(CONFIG)
    customers = config.table("customers")
    path = config.source_path(customers, "day1")
    assert Path(path) == ROOT / "data" / "landing" / "day1" / "customers.csv"
    assert Path(path).is_file()
    with pytest.raises(ConfigError, match="batch"):
        config.source_path(customers, None)
    config.source_root = "abfss://landing@acct.dfs.core.windows.net"
    assert config.source_path(customers, "d").startswith("abfss://landing@acct")


def test_disabled_tables_are_skipped():
    doc = cfg(enabled=False)
    other = copy.deepcopy(BASE["tables"][0])
    other["name"] = "u"
    doc["tables"].append(other)
    assert [t.name for t in parse_config(doc).enabled_tables] == ["u"]
