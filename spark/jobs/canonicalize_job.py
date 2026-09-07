#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from pyspark.sql import functions as F
from spark.common.io import storage_format
from spark.common.session import (DEFAULT_CONFIG, build_spark, load_config)
from spark.recon.canonicalize import canonicalize, SOURCE_SCHEMAS

SOURCES = ("internal", "processor", "bank")

def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--landing", default=None, help="override paths.landing")
    p.add_argument("--lake", default=None, help="override paths.canonical")
    p.add_argument("--config", default=DEFAULT_CONFIG)
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    
    # If the user passed explicit CLI paths, use them. 
    # Otherwise, fall back to the paths defined in whichever config file they specified.
    landing = a.landing or cfg["paths"]["landing_zone"] if "landing_zone" in cfg["paths"] else cfg["paths"]["landing"]
    canonical_root = a.lake or cfg["paths"]["canonical"]

    fmt = storage_format(cfg)
    spark = build_spark(f"canonicalize-{a.date}", cfg)
    try:
        for src in SOURCES:
            # We must handle both the S3 partition structure (landing_zone/src/...) 
            # and the local CI structure (/tmp/ci/landing/...)
            if landing.startswith("s3a://"):
                path = f"{landing}/{src}/{src}_{a.date}.csv"
            else:
                path = f"{landing}/{a.date}/{src}_{a.date}.csv"
                
            raw = (spark.read
                   .option("header", "true")
                   .schema(SOURCE_SCHEMAS[src])
                   .csv(path))
            
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
