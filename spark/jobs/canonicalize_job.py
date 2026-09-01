#!/usr/bin/env python3
"""canonicalize_job.py — landing CSVs -> canonical Delta tables.

Day 1 only. No matching, no classification, no control totals. The single claim
this job makes is: the canonical tables carry exactly the values the generator
emitted, in a different representation.

    python spark/jobs/canonicalize_job.py --date 2026-07-06

Reads   s3a://recon-landing/<date>/{internal,processor,bank}_<date>.csv
Writes  s3a://recon-lake/canonical/{internal,processor,bank}/  (Delta, partitioned
        by business_date so Day 2 can read a window without a full scan)
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from spark.common.session import build_spark, load_config          # noqa: E402
from spark.recon.canonicalize import canonicalize, SOURCE_SCHEMAS  # noqa: E402

SOURCES = ("internal", "processor", "bank")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--landing", default=None, help="override paths.landing")
    p.add_argument("--lake", default=None, help="override paths.canonical")
    a = p.parse_args(argv)

    cfg = load_config()
    landing = a.landing or cfg["paths"]["landing"]
    canonical_root = a.lake or cfg["paths"]["canonical"]

    spark = build_spark(f"canonicalize-{a.date}", cfg)
    try:
        for src in SOURCES:
            path = f"{landing}/{a.date}/{src}_{a.date}.csv"
            raw = (spark.read
                   .option("header", "true")
                   .schema(SOURCE_SCHEMAS[src])   # never inferSchema on money
                   .csv(path))
            canon = canonicalize(raw, src)
            (canon.write.format("delta")
             .mode("overwrite")
             .partitionBy("business_date")
             .save(f"{canonical_root}/{src}/"))
            n = spark.read.format("delta").load(f"{canonical_root}/{src}/").count()
            print(f"canonical/{src}: {n} rows written")
        print("canonicalization complete — run scripts/verify_day1.py next")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
