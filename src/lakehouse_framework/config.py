"""YAML metadata: one file describes every table the framework loads.

This module is pure Python (no Spark) so configuration errors are caught before a
cluster is started.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

LOAD_TYPES = ("scd1", "scd2", "insert_only")
_TYPE_RE = re.compile(
    r"^(string|int|bigint|smallint|double|float|boolean|date|timestamp|decimal\(\d+,\s*\d+\))$"
)


class ConfigError(ValueError):
    """Raised when the pipeline YAML is invalid."""


@dataclass(frozen=True)
class QualityRule:
    name: str
    rule: str  # Spark SQL boolean expression that valid rows satisfy


@dataclass
class TableConfig:
    name: str
    source_path: str
    source_format: str
    source_options: dict
    target: str
    load_type: str
    keys: list[str]
    schema: dict[str, str]
    watermark_column: str | None = None
    track_columns: list[str] = field(default_factory=list)
    quality: list[QualityRule] = field(default_factory=list)
    enabled: bool = True

    @property
    def required_columns(self) -> list[str]:
        cols = list(self.keys)
        if self.watermark_column and self.watermark_column not in cols:
            cols.append(self.watermark_column)
        return cols


@dataclass
class FrameworkConfig:
    tables: list[TableConfig]
    source_root: str | None = None

    @property
    def enabled_tables(self) -> list[TableConfig]:
        return [t for t in self.tables if t.enabled]

    def table(self, name: str) -> TableConfig:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(name)

    def source_path(self, table: TableConfig, batch: str | None = None) -> str:
        """Fill ``{batch}`` and prefix ``source_root`` for relative local paths."""
        path = table.source_path
        if "{batch}" in path:
            if batch is None:
                raise ConfigError(f"table '{table.name}': source path needs a batch value")
            path = path.replace("{batch}", batch)
        if self.source_root and not _is_absolute(path):
            path = os.path.join(self.source_root, path)
        return path


def _is_absolute(path: str) -> bool:
    return os.path.isabs(path) or "://" in path or path.startswith(("abfss:", "dbfs:"))


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ConfigError(msg)


def _parse_table(raw: dict, defaults: dict) -> TableConfig:
    _require(isinstance(raw, dict), "each entry under 'tables' must be a mapping")
    name = raw.get("name")
    _require(bool(name), "every table needs a 'name'")
    where = f"table '{name}'"

    source = raw.get("source") or {}
    _require(bool(source.get("path")), f"{where}: 'source.path' is required")
    load_type = raw.get("load_type")
    _require(load_type in LOAD_TYPES, f"{where}: load_type must be one of {', '.join(LOAD_TYPES)}")
    keys = raw.get("keys") or []
    _require(isinstance(keys, list) and len(keys) > 0, f"{where}: 'keys' must list at least one column")
    schema = raw.get("schema") or {}
    _require(isinstance(schema, dict) and len(schema) > 0, f"{where}: 'schema' is required")
    for col, typ in schema.items():
        _require(bool(_TYPE_RE.match(str(typ).strip().lower())), f"{where}: unsupported type '{typ}' for column '{col}'")
    for col in keys:
        _require(col in schema, f"{where}: key column '{col}' is not in the schema")

    wm = raw.get("watermark_column")
    _require(wm is None or wm in schema, f"{where}: watermark column '{wm}' is not in the schema")

    track = raw.get("track_columns")
    if load_type == "scd2" and not track:
        track = [c for c in schema if c not in keys and c != wm]
    track = track or []
    for col in track:
        _require(col in schema, f"{where}: track column '{col}' is not in the schema")

    rules = []
    seen = set()
    for r in raw.get("quality") or []:
        _require(isinstance(r, dict) and r.get("name") and r.get("rule"),
                 f"{where}: each quality rule needs 'name' and 'rule'")
        _require(r["name"] not in seen, f"{where}: duplicate quality rule '{r['name']}'")
        seen.add(r["name"])
        rules.append(QualityRule(r["name"], r["rule"]))

    options = dict(defaults.get("source_options") or {})
    options.update(source.get("options") or {})
    return TableConfig(
        name=name,
        source_path=source["path"],
        source_format=source.get("format", defaults.get("source_format", "csv")),
        source_options={k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in options.items()},
        target=raw.get("target", f"silver/{name}"),
        load_type=load_type,
        keys=list(keys),
        schema={c: str(t).strip().lower() for c, t in schema.items()},
        watermark_column=wm,
        track_columns=list(track),
        quality=rules,
        enabled=bool(raw.get("enabled", True)),
    )


def parse_config(doc: dict, base_dir: str | Path | None = None) -> FrameworkConfig:
    _require(isinstance(doc, dict), "config must be a mapping")
    defaults = doc.get("defaults") or {}
    tables_raw = doc.get("tables")
    _require(isinstance(tables_raw, list) and tables_raw, "config needs a non-empty 'tables' list")
    tables = [_parse_table(t, defaults) for t in tables_raw]
    names = [t.name for t in tables]
    dupes = sorted({n for n in names if names.count(n) > 1})
    _require(not dupes, f"duplicate table names: {', '.join(dupes)}")

    source_root = defaults.get("source_root")
    if source_root and base_dir is not None and not _is_absolute(source_root):
        source_root = os.path.normpath(os.path.join(str(base_dir), source_root))
    return FrameworkConfig(tables=tables, source_root=source_root)


def load_config(path: str | Path) -> FrameworkConfig:
    p = Path(path)
    with p.open(encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    return parse_config(doc, base_dir=p.resolve().parent)
