from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def storage_format(cfg: dict) -> str:
    return cfg.get("storage", {}).get("format", "delta")


def read_canonical(spark: SparkSession, cfg: dict, source: str,
                   business_date: str) -> DataFrame:
    """Read exactly the delivery for this run. Canonical columns unchanged."""
    root = cfg["paths"]["canonical"]
    df = spark.read.format(storage_format(cfg)).load(f"{root}/{source}/")
    return (df.filter(F.col("delivery_date") == F.lit(business_date).cast("date"))
              .drop("delivery_date"))


def read_recon_output(spark: SparkSession, cfg: dict,
                      business_date: str) -> DataFrame:
    path = cfg["paths"]["recon_output"]
    return (spark.read.format(storage_format(cfg)).load(path)
            .filter(F.col("business_date") == F.lit(business_date).cast("date")))
