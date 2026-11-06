# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata-driven Lakehouse framework: notebook entry point
# MAGIC
# MAGIC Works in **Azure Databricks** and **Microsoft Fabric** notebooks. The existing Spark
# MAGIC session (with Delta Lake) is reused.
# MAGIC
# MAGIC 1. Install the package on the cluster or environment:
# MAGIC    `%pip install git+https://github.com/someshbvrm/metadata-driven-lakehouse-framework`
# MAGIC 2. Point `CONFIG_PATH` at your pipeline YAML and `TABLE_ROOT` at your lakehouse.
# MAGIC    - Fabric: `TABLE_ROOT = "Tables"` (default lakehouse), `SOURCE_ROOT = "Files"`
# MAGIC    - Databricks: `TABLE_ROOT = "abfss://silver@<account>.dfs.core.windows.net/framework"`

# COMMAND ----------

CONFIG_PATH = "/Workspace/Repos/<you>/metadata-driven-lakehouse-framework/config/pipeline.yaml"
TABLE_ROOT = "abfss://lakehouse@<account>.dfs.core.windows.net/framework"
SOURCE_ROOT = "abfss://landing@<account>.dfs.core.windows.net"
BATCH = "day1"  # e.g. pass from a pipeline parameter or widget

# COMMAND ----------

from lakehouse_framework import load_config
from lakehouse_framework.engine import LakehouseFramework

config = load_config(CONFIG_PATH)
config.source_root = SOURCE_ROOT  # override the repo-relative default
framework = LakehouseFramework(config, spark, root=TABLE_ROOT)  # noqa: F821 - notebook global

results = framework.run(batch=BATCH)
display(spark.createDataFrame([r.__dict__ for r in results]))  # noqa: F821

# COMMAND ----------

# Fail the notebook (and the calling pipeline) if any table failed
failed = [r.table_name for r in results if r.status == "FAILED"]
if failed:
    raise RuntimeError(f"Tables failed: {failed}. See {framework.audit_path}")
