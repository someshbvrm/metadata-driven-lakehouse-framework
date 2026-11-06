"""Command line: ``python -m lakehouse_framework --config config/pipeline.yaml --batch day1``."""
from __future__ import annotations

import argparse
import sys

from .config import ConfigError, load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse_framework")
    parser.add_argument("--config", required=True, help="pipeline YAML")
    parser.add_argument("--batch", help="value for {batch} in source paths")
    parser.add_argument("--root", default="lakehouse", help="where Delta tables are written")
    parser.add_argument("--table", action="append", help="only run this table (repeatable)")
    parser.add_argument("--validate", action="store_true", help="validate the config and exit")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    if args.validate:
        print(f"config OK: {len(config.tables)} tables ({', '.join(t.name for t in config.tables)})")
        return 0

    from .engine import LakehouseFramework
    from .spark import get_spark

    framework = LakehouseFramework(config, get_spark(), root=args.root)
    results = framework.run(batch=args.batch, tables=args.table)
    for r in results:
        print(
            f"{r.table_name:<12} {r.status:<9} read={r.rows_read} inserted={r.rows_inserted} "
            f"updated={r.rows_updated} quarantined={r.rows_quarantined} "
            f"skipped={r.rows_skipped_watermark} dupes={r.rows_duplicates}"
            + (f"  error={r.error}" if r.error else "")
        )
    return 1 if any(r.status == "FAILED" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
