"""End-to-end tests on local Spark + Delta Lake (run in CI; skipped if PySpark is absent)."""
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse_framework import load_config  # noqa: E402
from lakehouse_framework.engine import LakehouseFramework  # noqa: E402

CONFIG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    """Run day1 then day2 once for the whole module."""
    config = load_config(CONFIG)
    fw = LakehouseFramework(config, spark, root=str(tmp_path_factory.mktemp("lake")))
    day1 = {r.table_name: r for r in fw.run(batch="day1")}
    snap = {
        name: spark.read.format("delta").load(fw.table_path(config.table(name))).collect()
        for name in ("customers", "products", "orders")
    }
    day2 = {r.table_name: r for r in fw.run(batch="day2")}
    return fw, config, day1, day2, snap


def table(spark, fw, config, name):
    return spark.read.format("delta").load(fw.table_path(config.table(name)))


def test_day1_loads_all_tables(lake):
    _, _, day1, _, snap = lake
    assert all(r.status == "SUCCEEDED" for r in day1.values()), [r.error for r in day1.values()]
    assert len(snap["customers"]) == 4  # 7 read: 1 duplicate, 2 failed quality
    assert len(snap["products"]) == 3
    assert len(snap["orders"]) == 3


def test_duplicates_keep_latest_version(lake):
    _, _, day1, _, snap = lake
    assert day1["customers"].rows_duplicates == 1
    meera = [r for r in snap["customers"] if r["customer_id"] == 3]
    assert len(meera) == 1 and meera[0]["email"] == "meera.iyer@example.com"


def test_quality_failures_are_quarantined_with_rule_names(spark, lake):
    fw, _, day1, _, _ = lake
    assert day1["customers"].rows_quarantined == 2
    q = spark.read.format("delta").load(fw.quarantine_path)
    rules = {
        tuple(r["failed_rules"])
        for r in q.filter(F.col("stage") == "quality").collect()
    }
    assert ("email_present", "email_format") in rules or ("email_present",) in rules
    assert ("email_format",) in rules
    assert ("positive_quantity", "positive_amount") in rules
    assert ("positive_price",) in rules


def test_type_errors_are_rejected_before_loading(spark, lake):
    fw, _, _, day2, _ = lake
    assert day2["customers"].rows_rejected_schema == 1
    q = spark.read.format("delta").load(fw.quarantine_path).filter(F.col("stage") == "schema").collect()
    assert [list(r["failed_rules"]) for r in q] == [["type:updated_at"]]


def test_scd2_closes_old_version_and_adds_new(spark, lake):
    fw, config, _, day2, _ = lake
    dim = table(spark, fw, config, "customers")
    vikram = dim.filter("customer_id = 2").orderBy("effective_from").collect()
    assert [r["city"] for r in vikram] == ["Pune", "Bengaluru"]
    assert [r["is_current"] for r in vikram] == [False, True]
    assert vikram[0]["effective_to"] == vikram[1]["effective_from"]
    # John's attributes did not change, so no new version even though updated_at moved
    assert dim.filter("customer_id = 4").count() == 1
    assert dim.count() == 6 and dim.filter("is_current").count() == 5
    assert day2["customers"].rows_updated == 1


def test_scd1_updates_in_place(spark, lake):
    fw, config, _, day2, _ = lake
    dim = table(spark, fw, config, "products")
    assert dim.count() == 4
    price = dim.filter("product_id = 102").first()["unit_price"]
    assert float(price) == 345.0
    assert (day2["products"].rows_inserted, day2["products"].rows_updated) == (1, 1)


def test_insert_only_ignores_existing_keys(spark, lake):
    fw, config, _, day2, _ = lake
    fact = table(spark, fw, config, "orders")
    assert fact.count() == 5
    assert fact.filter("order_id = 5003").first()["order_ts"].day == 1  # original kept
    assert day2["orders"].rows_inserted == 2


def test_watermark_skips_processed_rows_and_reruns_are_idempotent(spark, lake):
    fw, config, _, day2, _ = lake
    assert day2["customers"].rows_skipped_watermark == 1  # Asha's unchanged 2026-01-01 row
    before = {n: table(spark, fw, config, n).count() for n in ("customers", "products", "orders")}
    rerun = {r.table_name: r for r in fw.run(batch="day2")}
    after = {n: table(spark, fw, config, n).count() for n in ("customers", "products", "orders")}
    assert before == after
    assert all(r.rows_inserted == 0 and r.rows_updated == 0 for r in rerun.values())


def test_audit_log_records_every_load_and_failures(spark, lake):
    fw, _, _, _, _ = lake
    bad = fw.run(batch="missing_batch", tables=["products"])
    assert bad[0].status == "FAILED" and "missing_batch" in bad[0].error
    audit = spark.read.format("delta").load(fw.audit_path)
    statuses = {(r["batch"], r["table_name"]): r["status"] for r in audit.collect()}
    assert statuses[("day1", "customers")] == "SUCCEEDED"
    assert statuses[("missing_batch", "products")] == "FAILED"
    assert audit.filter("batch = 'day1'").count() == 3
