"""Run every configured table: read -> type -> watermark -> de-duplicate -> quality -> MERGE -> audit."""
from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import FrameworkConfig, TableConfig
from .merge import STRATEGIES
from .transform import (
    apply_quality,
    apply_schema,
    deduplicate,
    filter_watermark,
    to_quarantine,
)

AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("batch", T.StringType()),
        T.StructField("table_name", T.StringType()),
        T.StructField("load_type", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("rows_read", T.LongType()),
        T.StructField("rows_rejected_schema", T.LongType()),
        T.StructField("rows_skipped_watermark", T.LongType()),
        T.StructField("rows_duplicates", T.LongType()),
        T.StructField("rows_quarantined", T.LongType()),
        T.StructField("rows_inserted", T.LongType()),
        T.StructField("rows_updated", T.LongType()),
        T.StructField("watermark_before", T.StringType()),
        T.StructField("watermark_after", T.StringType()),
        T.StructField("started_at", T.TimestampType()),
        T.StructField("finished_at", T.TimestampType()),
        T.StructField("duration_s", T.DoubleType()),
        T.StructField("error", T.StringType()),
    ]
)

WATERMARK_SCHEMA = T.StructType(
    [
        T.StructField("table_name", T.StringType()),
        T.StructField("watermark_value", T.StringType()),
        T.StructField("updated_at", T.TimestampType()),
    ]
)


@dataclass
class TableResult:
    run_id: str
    batch: str | None
    table_name: str
    load_type: str
    status: str = "RUNNING"
    rows_read: int = 0
    rows_rejected_schema: int = 0
    rows_skipped_watermark: int = 0
    rows_duplicates: int = 0
    rows_quarantined: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    watermark_before: str | None = None
    watermark_after: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    duration_s: float = 0.0
    error: str | None = None


class LakehouseFramework:
    """Metadata-driven loader. ``root`` is where Delta tables live (local path, abfss:// or Tables/)."""

    def __init__(self, config: FrameworkConfig, spark: SparkSession, root: str = "lakehouse"):
        self.config = config
        self.spark = spark
        self.root = root.rstrip("/")

    # ---- paths -----------------------------------------------------------------
    def table_path(self, table: TableConfig) -> str:
        return f"{self.root}/{table.target}"

    @property
    def audit_path(self) -> str:
        return f"{self.root}/_framework/audit_log"

    @property
    def quarantine_path(self) -> str:
        return f"{self.root}/_framework/quarantine"

    @property
    def watermark_path(self) -> str:
        return f"{self.root}/_framework/watermarks"

    # ---- watermarks ----------------------------------------------------------------
    def get_watermark(self, table_name: str) -> str | None:
        if not DeltaTable.isDeltaTable(self.spark, self.watermark_path):
            return None
        row = (
            self.spark.read.format("delta").load(self.watermark_path)
            .filter(F.col("table_name") == table_name)
            .select("watermark_value")
            .first()
        )
        return row[0] if row else None

    def set_watermark(self, table_name: str, value: str) -> None:
        df = self.spark.createDataFrame(
            [(table_name, value, datetime.now(timezone.utc))], WATERMARK_SCHEMA
        )
        if not DeltaTable.isDeltaTable(self.spark, self.watermark_path):
            df.write.format("delta").mode("overwrite").save(self.watermark_path)
            return
        (
            DeltaTable.forPath(self.spark, self.watermark_path).alias("t")
            .merge(df.alias("s"), "t.table_name = s.table_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ---- run -----------------------------------------------------------------------
    def run(self, batch: str | None = None, tables: list[str] | None = None,
            run_id: str | None = None) -> list[TableResult]:
        run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        selected = [t for t in self.config.enabled_tables if tables is None or t.name in tables]
        results = []
        for table in selected:
            result = self.run_table(table, run_id, batch)
            self._write_audit(result)
            results.append(result)
        return results

    def run_table(self, table: TableConfig, run_id: str, batch: str | None) -> TableResult:
        res = TableResult(run_id=run_id, batch=batch, table_name=table.name, load_type=table.load_type)
        t0 = time.monotonic()
        try:
            self._load(table, res, batch)
            res.status = "SUCCEEDED"
        except Exception as exc:  # noqa: BLE001 - recorded in the audit log, run continues
            res.status = "FAILED"
            res.error = f"{type(exc).__name__}: {exc}"[:2000]
        res.finished_at = datetime.now(timezone.utc)
        res.duration_s = round(time.monotonic() - t0, 3)
        return res

    def _load(self, table: TableConfig, res: TableResult, batch: str | None) -> None:
        path = self.config.source_path(table, batch)
        reader = self.spark.read.format(table.source_format).options(**table.source_options)
        raw = reader.load(path)
        res.rows_read = raw.count()

        typed, rejected = apply_schema(raw, table)
        typed = typed.cache()
        res.rows_rejected_schema = rejected.count()
        if res.rows_rejected_schema:
            self._quarantine(to_quarantine(rejected, table.name, res.run_id, "schema"))

        res.watermark_before = self.get_watermark(table.name)
        fresh = filter_watermark(typed, table, res.watermark_before)
        fresh_count = fresh.count()
        res.rows_skipped_watermark = typed.count() - fresh_count

        unique = deduplicate(fresh, table)
        res.rows_duplicates = fresh_count - unique.count()

        valid, bad = apply_quality(unique, table.quality)
        bad_count = bad.count()
        if bad_count:
            self._quarantine(to_quarantine(bad, table.name, res.run_id, "quality"))
        res.rows_quarantined = res.rows_rejected_schema + bad_count

        if fresh_count:
            to_load = (
                valid.withColumn("_run_id", F.lit(res.run_id))
                .withColumn("_ingested_at", F.current_timestamp())
            )
            if to_load.limit(1).count():
                res.rows_inserted, res.rows_updated = STRATEGIES[table.load_type](
                    self.spark, to_load, self.table_path(table), table
                )
            if table.watermark_column:
                new_wm = fresh.agg(F.max(table.watermark_column)).first()[0]
                if new_wm is not None:
                    res.watermark_after = str(new_wm)
                    self.set_watermark(table.name, res.watermark_after)
        if res.watermark_after is None:
            res.watermark_after = res.watermark_before
        typed.unpersist()

    def _quarantine(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").save(self.quarantine_path)

    def _write_audit(self, res: TableResult) -> None:
        row = asdict(res)
        df = self.spark.createDataFrame([tuple(row[f.name] for f in AUDIT_SCHEMA.fields)], AUDIT_SCHEMA)
        df.write.format("delta").mode("append").save(self.audit_path)
