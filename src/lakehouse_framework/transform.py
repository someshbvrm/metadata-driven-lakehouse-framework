"""Row-level steps before the merge: typing, watermark filter, de-duplication, data quality."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import QualityRule, TableConfig

DQ_COL = "_dq_failed"


def _failed_array(checks) -> F.Column:
    """Array of failed check names (nulls removed)."""
    if not checks:
        return F.array().cast("array<string>")
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def apply_schema(df: DataFrame, table: TableConfig) -> tuple[DataFrame, DataFrame]:
    """Cast every configured column with ``try_cast``.

    Returns ``(typed, rejected)``. A row is rejected when a non-empty value cannot be
    cast to its type, or when a key / watermark column is empty. Rejected rows keep
    their original (string) values so they can be inspected in quarantine.
    """
    missing = [c for c in table.schema if c not in df.columns]
    if missing:
        raise ValueError(f"source for '{table.name}' is missing columns: {', '.join(missing)}")

    raw = df.select([F.col(c).alias(c) for c in table.schema])
    typed_cols = {
        c: F.expr(f"try_cast(`{c}` AS {t})") for c, t in table.schema.items()
    }
    checks = []
    for c in table.schema:
        checks.append(
            F.when(F.col(c).isNotNull() & typed_cols[c].isNull(), F.lit(f"type:{c}"))
        )
    for c in table.required_columns:
        checks.append(F.when(F.col(c).isNull(), F.lit(f"required:{c}")))

    flagged = raw.withColumn(DQ_COL, _failed_array(checks))
    rejected = flagged.filter(F.size(DQ_COL) > 0)
    typed = flagged.filter(F.size(DQ_COL) == 0).select(
        [typed_cols[c].alias(c) for c in table.schema]
    )
    return typed, rejected


def filter_watermark(df: DataFrame, table: TableConfig, last_value: str | None) -> DataFrame:
    """Keep rows newer than the last processed watermark."""
    if not table.watermark_column or last_value is None:
        return df
    wm_type = table.schema[table.watermark_column]
    return df.filter(F.col(table.watermark_column) > F.lit(last_value).cast(wm_type))


def deduplicate(df: DataFrame, table: TableConfig) -> DataFrame:
    """Keep one row per key: the one with the latest watermark (or any, if none)."""
    order = (
        F.col(table.watermark_column).desc_nulls_last()
        if table.watermark_column
        else F.monotonically_increasing_id().desc()
    )
    w = Window.partitionBy(*table.keys).orderBy(order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_quality(df: DataFrame, rules: list[QualityRule]) -> tuple[DataFrame, DataFrame]:
    """Split rows into ``(valid, quarantined)``.

    A rule is a Spark SQL boolean expression valid rows satisfy; a NULL result counts
    as a failure. Quarantined rows carry the names of every rule they broke.
    """
    checks = [
        F.when(~F.coalesce(F.expr(r.rule), F.lit(False)), F.lit(r.name)) for r in rules
    ]
    flagged = df.withColumn(DQ_COL, _failed_array(checks))
    valid = flagged.filter(F.size(DQ_COL) == 0).drop(DQ_COL)
    bad = flagged.filter(F.size(DQ_COL) > 0)
    return valid, bad


def to_quarantine(bad: DataFrame, table_name: str, run_id: str, stage: str) -> DataFrame:
    data_cols = [c for c in bad.columns if c != DQ_COL]
    return bad.select(
        F.lit(table_name).alias("table_name"),
        F.lit(run_id).alias("run_id"),
        F.lit(stage).alias("stage"),
        F.col(DQ_COL).alias("failed_rules"),
        F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in data_cols])).alias("record"),
        F.current_timestamp().alias("quarantined_at"),
    )
