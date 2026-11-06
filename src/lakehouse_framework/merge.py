"""Delta Lake MERGE strategies: SCD Type 1, SCD Type 2 and insert-only."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import TableConfig

SCD2_COLUMNS = ("_row_hash", "effective_from", "effective_to", "is_current")


def _key_condition(keys, left="t", right="s") -> str:
    return " AND ".join(f"{left}.`{k}` = {right}.`{k}`" for k in keys)


def _last_merge_metrics(target: DeltaTable) -> tuple[int, int]:
    metrics = target.history(1).select("operationMetrics").first()[0] or {}
    return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))


def _initial_write(df: DataFrame, path: str) -> tuple[int, int]:
    df.write.format("delta").mode("overwrite").save(path)
    return df.count(), 0


def merge_scd1(spark: SparkSession, df: DataFrame, path: str, table: TableConfig) -> tuple[int, int]:
    """Upsert: update changed rows in place, insert new keys."""
    if not DeltaTable.isDeltaTable(spark, path):
        return _initial_write(df, path)
    target = DeltaTable.forPath(spark, path)
    data_cols = [c for c in table.schema if c not in table.keys]
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in data_cols) or "false"
    (
        target.alias("t")
        .merge(df.alias("s"), _key_condition(table.keys))
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _last_merge_metrics(target)


def merge_insert_only(spark: SparkSession, df: DataFrame, path: str, table: TableConfig) -> tuple[int, int]:
    """Insert new keys only; rows whose key already exists are ignored (idempotent re-runs)."""
    if not DeltaTable.isDeltaTable(spark, path):
        return _initial_write(df, path)
    target = DeltaTable.forPath(spark, path)
    target.alias("t").merge(df.alias("s"), _key_condition(table.keys)).whenNotMatchedInsertAll().execute()
    return _last_merge_metrics(target)


def with_row_hash(df: DataFrame, track_columns: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in track_columns]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def merge_scd2(spark: SparkSession, df: DataFrame, path: str, table: TableConfig) -> tuple[int, int]:
    """Type 2 history: close the current version of a changed row and insert a new one.

    Uses the staged-union pattern so one MERGE both expires the old version and inserts
    the new version: changed rows appear twice in the source, once with the business
    key as merge key (to match and close the current row) and once with NULL merge keys
    (never matches, so it is inserted as the new current row).
    """
    effective = (
        F.col(table.watermark_column).cast("timestamp") if table.watermark_column else F.current_timestamp()
    )
    src = with_row_hash(df, table.track_columns).withColumn("_effective", effective)
    out_cols = list(df.columns)

    if not DeltaTable.isDeltaTable(spark, path):
        first = src.select(
            *[F.col(c) for c in out_cols],
            F.col("_row_hash"),
            F.col("_effective").alias("effective_from"),
            F.lit(None).cast("timestamp").alias("effective_to"),
            F.lit(True).alias("is_current"),
        )
        return _initial_write(first, path)

    target = DeltaTable.forPath(spark, path)
    current = target.toDF().filter("is_current")
    changed = (
        src.alias("s")
        .join(current.alias("t"), [F.col(f"s.{k}") == F.col(f"t.{k}") for k in table.keys])
        .where(F.col("s._row_hash") != F.col("t._row_hash"))
        .select("s.*")
    )
    key_types = dict(df.dtypes)
    mk = [f"_mk_{i}" for i in range(len(table.keys))]
    as_new = changed.select("*", *[F.lit(None).cast(key_types[k]).alias(m) for k, m in zip(table.keys, mk)])
    as_match = src.select("*", *[F.col(k).alias(m) for k, m in zip(table.keys, mk)])
    staged = as_match.unionByName(as_new)

    condition = " AND ".join(f"t.`{k}` = s.`{m}`" for k, m in zip(table.keys, mk)) + " AND t.is_current = true"
    insert_values = {c: f"s.`{c}`" for c in out_cols}
    insert_values.update(
        {
            "_row_hash": "s._row_hash",
            "effective_from": "s._effective",
            "effective_to": "CAST(NULL AS TIMESTAMP)",
            "is_current": "true",
        }
    )
    (
        target.alias("t")
        .merge(staged.alias("s"), condition)
        .whenMatchedUpdate(
            condition="t._row_hash <> s._row_hash",
            set={"is_current": "false", "effective_to": "s._effective"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    return _last_merge_metrics(target)


STRATEGIES = {
    "scd1": merge_scd1,
    "scd2": merge_scd2,
    "insert_only": merge_insert_only,
}
