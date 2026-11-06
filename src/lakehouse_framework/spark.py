"""Spark session helper.

On Databricks or Microsoft Fabric a session already exists and is reused. Locally
(and in CI) a Delta-enabled session is created with ``delta-spark``.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse-framework") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
