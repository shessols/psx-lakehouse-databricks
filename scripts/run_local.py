"""Run the whole pipeline on your laptop with plain Spark + Parquet (no Databricks needed).

    python scripts/run_local.py                                   # downloads the public PSX dataset
    python scripts/run_local.py --source /path/to/psx-market-data-platform/dataset
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import SparkSession  # noqa: E402

from psx_lakehouse import ingest, pipeline  # noqa: E402
from psx_lakehouse.config import SOURCE_BASE  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="local dataset folder (default: GitHub)")
    ap.add_argument("--out", default=str(ROOT / "local"))
    a = ap.parse_args()
    base = Path(a.source).resolve().as_uri() if a.source else SOURCE_BASE
    volume, tables = f"{a.out}/volume", f"{a.out}/tables"
    os.makedirs(volume, exist_ok=True)

    spark = (SparkSession.builder.master("local[*]").appName("psx-lakehouse")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    store = pipeline.LocalStore(spark, tables)

    t = time.time()
    print("ingest :", ingest.land(volume, base=base))
    print("bronze :", pipeline.bronze_batch(spark, store, volume), "rows")
    print("silver :", pipeline.silver(store))
    print("gold   :", pipeline.gold(store))
    for c in pipeline.quality_checks(store):
        print(f"  [{'PASS' if c['passed'] else 'FAIL'}] {c['check']}: {c['value']}")
    print("\nStrategy summary (fees and tax not included):")
    store.read("gold_strategy_summary").show(truncate=False)
    print(f"done in {time.time() - t:.0f}s -> {tables}")


if __name__ == "__main__":
    main()
