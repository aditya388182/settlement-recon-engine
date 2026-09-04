#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from pyspark.sql import functions as F                             # noqa: E402
from spark.common.io import storage_format                         # noqa: E402
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

    fmt = storage_format(cfg)
    spark = build_spark(f"canonicalize-{a.date}", cfg)
    try:
        for src in SOURCES:
            path = f"{landing}/{a.date}/{src}_{a.date}.csv"
            raw = (spark.read
                   .option("header", "true")
                   .schema(SOURCE_SCHEMAS[src])   # never inferSchema on money
                   .csv(path))
            # delivery_date = the date of the FILE we were sent, which is what
            # a run owns. A bank delivery for D legitimately contains rows whose
            # own business_date is D+1 or D+2 (settlement lag); those rows still
            # belong to D's reconciliation, because D is when they arrived.
            canon = canonicalize(raw, src).withColumn(
                "delivery_date", F.lit(a.date).cast("date"))
            writer = (canon.write.format(fmt).mode("overwrite")
                      .partitionBy("delivery_date"))
            if fmt == "delta":
                writer = writer.option("replaceWhere",
                                       f"delivery_date = '{a.date}'")
            else:
                spark.conf.set("spark.sql.sources.partitionOverwriteMode",
                               "dynamic")
            writer.save(f"{canonical_root}/{src}/")
            n = canon.count()
            print(f"canonical/{src}: {n} rows written")
        print("canonicalization complete — run scripts/verify_day1.py next")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
