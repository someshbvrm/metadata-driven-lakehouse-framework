# Metadata-Driven Lakehouse Framework

[![CI](https://github.com/someshbvrm/metadata-driven-lakehouse-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/someshbvrm/metadata-driven-lakehouse-framework/actions/workflows/ci.yml)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**Live run report:** https://someshbvrm.github.io/metadata-driven-lakehouse-framework/
(rebuilt by CI on every push from a real PySpark + Delta Lake run)

A small, production-style **PySpark + Delta Lake** framework where **one YAML file drives every
table**. Adding a table means adding a config entry, not writing a new notebook. It runs locally,
on **Azure Databricks** and in **Microsoft Fabric** notebooks.

| Capability | How |
|---|---|
| Config-driven loads | `config/pipeline.yaml` declares the source, target, keys, schema, load type and rules for each table, validated before Spark starts |
| Schema enforcement | Every column is `try_cast` to its configured type; bad values are rejected instead of failing the job |
| Incremental loads | A per-table **watermark** (stored in Delta) skips rows already processed |
| De-duplication | One row per business key, keeping the latest by watermark |
| **SCD Type 2** | Single Delta **MERGE** that expires the current version and inserts the new one (staged-union pattern), with `effective_from`, `effective_to` and `is_current` |
| SCD Type 1 | MERGE that updates only rows whose values changed |
| Insert-only facts | MERGE that inserts new keys and ignores re-sent ones, so re-runs are idempotent |
| Data quality | Spark SQL rules per table; failing rows go to a **quarantine** Delta table with the names of every rule they broke |
| Audit | One row per table per run in a Delta **audit log**: rows read, skipped, de-duplicated, quarantined, inserted and updated, plus timings and errors |
| Fault isolation | A failing table is logged as `FAILED` and the other tables still load |

## Architecture

```text
 landing files ──► read ──► type cast ──► watermark ──► de-duplicate ──► quality rules ──► Delta MERGE
                              │              filter                          │          (scd1 / scd2 /
                              ▼                                              ▼           insert_only)
                       quarantine (schema)                          quarantine (quality)       │
                                                                                               ▼
                         _framework/watermarks  ◄────────── update ◄──────────────── silver tables
                         _framework/audit_log   ◄────────── one row per table per run
```

## Configuration

```yaml
tables:
  - name: customers
    source:
      path: data/landing/{batch}/customers.csv
    target: silver/dim_customer
    load_type: scd2
    keys: [customer_id]
    watermark_column: updated_at
    track_columns: [name, email, city, segment]   # changes here create a new version
    schema:
      customer_id: int
      email: string
      updated_at: timestamp
      # ...
    quality:
      - name: email_present
        rule: email IS NOT NULL
```

`load_type` is `scd1`, `scd2` or `insert_only`. `{batch}` in a source path is filled at run time.
See [`config/pipeline.yaml`](config/pipeline.yaml) for the full sample.

## What the sample run shows

The CI run loads batch `day1`, then `day2`, then `day2` again:

- **Customers (SCD2):** a duplicate is collapsed to its latest version, and two rows fail email rules and
  are quarantined. On day 2, customer 2 moves city, so the old version is closed and a new one opens.
  Customer 4's unchanged row creates no new version, and a row with an invalid timestamp is rejected.
- **Products (SCD1):** a negative price is quarantined, and a price change on day 2 is applied in place.
- **Orders (insert-only):** an order with zero quantity and amount breaks two rules, and an order re-sent on
  day 2 is ignored.
- **Re-running day 2** changes nothing: the watermarks skip every row.

## Run it

```bash
pip install -e ".[dev]"          # PySpark 3.5 + delta-spark 3.2 (needs Java 17)
python -m lakehouse_framework --config config/pipeline.yaml --validate
python -m lakehouse_framework --config config/pipeline.yaml --batch day1 --root lakehouse
python -m lakehouse_framework --config config/pipeline.yaml --batch day2 --root lakehouse
python scripts/run_demo.py --out site     # full demo + HTML report
```

### Databricks / Microsoft Fabric

Install the package on the cluster or environment, then use
[`notebooks/run_framework.py`](notebooks/run_framework.py). It reuses the notebook's Spark session.
Point `TABLE_ROOT` at your lakehouse (`Tables` in Fabric, an `abfss://` path in Databricks) and
`SOURCE_ROOT` at the landing zone. A failed table fails the notebook, so the calling Data Factory
or Fabric pipeline sees the failure.

## Tests

```bash
pytest -v
```

- `tests/test_config.py`: config validation, pure Python with no Spark needed.
- `tests/test_pipeline_spark.py`: end-to-end on local Spark + Delta. It covers de-duplication, quarantine,
  type rejection, SCD2 history, SCD1 updates, insert-only idempotency, watermarks, re-runs and the audit log.
  These tests skip automatically if PySpark isn't installed.

## Project layout

```text
config/pipeline.yaml              # table metadata
data/landing/day1, day2/          # sample source batches
src/lakehouse_framework/
  config.py      # YAML parsing and validation (no Spark)
  transform.py   # typing, watermark filter, de-duplication, quality split
  merge.py       # Delta MERGE: scd1, scd2, insert_only
  engine.py      # orchestration, watermarks, quarantine, audit log
  spark.py       # session helper (reuses Databricks/Fabric session)
notebooks/run_framework.py        # Databricks / Fabric entry point
scripts/run_demo.py               # sample run + HTML report (published by CI)
```

## License

MIT © Someswara Rao K V
