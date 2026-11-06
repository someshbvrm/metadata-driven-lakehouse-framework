"""Run the sample pipeline for two daily batches and publish an HTML run report.

    python scripts/run_demo.py --out site

CI runs this after the tests and deploys ``site/`` to GitHub Pages.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timezone
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_framework import load_config  # noqa: E402
from lakehouse_framework.engine import LakehouseFramework  # noqa: E402
from lakehouse_framework.spark import get_spark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

CSS = """
:root{--bg:#f6f7f9;--card:#fff;--ink:#17202a;--muted:#5d6b7a;--line:#e3e7ec;--ok:#1f8a4c;--bad:#c0392b;--accent:#0f6cbd}
@media (prefers-color-scheme:dark){:root{--bg:#0f1419;--card:#18202a;--ink:#e6edf3;--muted:#9aa7b4;--line:#2a3542;--ok:#3fb56f;--bad:#e5604f;--accent:#4aa3ff}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1100px;margin:0 auto;padding:32px 16px 64px}h1{font-size:26px;margin:0 0 4px}
h2{font-size:18px;margin:36px 0 8px}.sub{color:var(--muted);margin:0 0 16px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-top:20px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.card b{display:block;font-size:26px}.card span{color:var(--muted);font-size:13px}
.tbl{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{text-align:left;padding:7px 12px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.03em}
tr:last-child td{border-bottom:0}.ok{color:var(--ok);font-weight:600}.bad{color:var(--bad);font-weight:600}
code{font-size:13px}footer{margin-top:40px;color:var(--muted);font-size:13px}a{color:var(--accent)}
"""


def html_table(rows, columns, status_col=None) -> str:
    head = "".join(f"<th>{escape(c)}</th>" for c in columns)
    body = []
    for r in rows:
        cells = []
        for c in columns:
            v = r[c]
            text = "" if v is None else str(v)
            cls = ""
            if c == status_col:
                cls = " class='ok'" if text in ("SUCCEEDED", "true", "True") else " class='bad'"
            cells.append(f"<td{cls}>{escape(text)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<div class='tbl'><table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="site")
    parser.add_argument("--root", default="demo-lakehouse")
    parser.add_argument("--repo-url", default="")
    args = parser.parse_args()

    shutil.rmtree(args.root, ignore_errors=True)
    spark = get_spark("lakehouse-framework-demo")
    config = load_config(ROOT / "config" / "pipeline.yaml")
    fw = LakehouseFramework(config, spark, root=args.root)

    for batch in ("day1", "day2", "day2"):  # the repeated day2 run proves re-runs are idempotent
        for r in fw.run(batch=batch):
            print(f"{batch} {r.table_name:<10} {r.status} ins={r.rows_inserted} upd={r.rows_updated} q={r.rows_quarantined}")

    read = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    audit = read(fw.audit_path).orderBy("started_at")
    audit_cols = ["batch", "table_name", "load_type", "status", "rows_read", "rows_skipped_watermark",
                  "rows_duplicates", "rows_quarantined", "rows_inserted", "rows_updated", "duration_s"]
    audit_rows = audit.select(*audit_cols).collect()

    quarantine = read(fw.quarantine_path).select(
        "table_name", "stage", F.concat_ws(", ", "failed_rules").alias("failed_rules"), "record"
    ).orderBy("table_name").collect()

    dim_customer = read(fw.table_path(config.table("customers"))).select(
        "customer_id", "name", "city", "email", "effective_from", "effective_to", "is_current"
    ).orderBy("customer_id", "effective_from").collect()
    dim_product = read(fw.table_path(config.table("products"))).select(
        "product_id", "product_name", "category", "unit_price", "updated_at"
    ).orderBy("product_id").collect()
    fact_orders = read(fw.table_path(config.table("orders"))).select(
        "order_id", "customer_id", "product_id", "quantity", "amount", "order_ts"
    ).orderBy("order_id").collect()

    runs = audit.select("run_id").distinct().count()
    failed = sum(1 for r in audit_rows if r["status"] != "SUCCEEDED")
    cards = [
        (runs, "framework runs"),
        (len(audit_rows), "table loads audited"),
        (failed, "failed loads"),
        (len(quarantine), "rows quarantined"),
        (sum(1 for r in dim_customer if not r["is_current"]), "SCD2 versions closed"),
        (len(fact_orders), "orders loaded"),
    ]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    repo = f' · <a href="{escape(args.repo_url)}">Source on GitHub</a>' if args.repo_url else ""
    html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Lakehouse framework run report</title>
<style>{CSS}</style></head><body><main>
<h1>Metadata-driven Lakehouse framework: run report</h1>
<p class="sub">Produced by CI on every push: the sample pipeline in <code>config/pipeline.yaml</code> runs for
batch <code>day1</code>, then <code>day2</code>, then <code>day2</code> again to show re-runs change nothing.
PySpark + Delta Lake.</p>
<div class="cards">{''.join(f'<div class="card"><b>{v}</b><span>{l}</span></div>' for v, l in cards)}</div>
<h2>Audit log</h2><p class="sub">One row per table per run, from <code>_framework/audit_log</code>.</p>
{html_table(audit_rows, audit_cols, status_col="status")}
<h2>dim_customer (SCD Type 2)</h2><p class="sub">Customer 2 moved from Pune to Bengaluru: the old version is closed and a new current version added.</p>
{html_table(dim_customer, ["customer_id", "name", "city", "email", "effective_from", "effective_to", "is_current"], status_col="is_current")}
<h2>dim_product (SCD Type 1)</h2><p class="sub">Product 102's price was updated in place; no history kept.</p>
{html_table(dim_product, ["product_id", "product_name", "category", "unit_price", "updated_at"])}
<h2>fact_orders (insert-only)</h2><p class="sub">Order 5003 was re-sent on day 2 and ignored because its key already existed.</p>
{html_table(fact_orders, ["order_id", "customer_id", "product_id", "quantity", "amount", "order_ts"])}
<h2>Quarantine</h2><p class="sub">Rows that failed a type check or a data-quality rule, with the rules they broke.</p>
{html_table(quarantine, ["table_name", "stage", "failed_rules", "record"])}
<footer>Generated {stamp}{repo}</footer>
</main></body></html>
"""
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "index.html").write_text(html, encoding="utf-8")
    print(f"wrote {out / 'index.html'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
