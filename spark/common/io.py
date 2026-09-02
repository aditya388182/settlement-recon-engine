from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def storage_format(cfg: dict) -> str:
    return cfg.get("storage", {}).get("format", "delta")


def read_canonical(spark: SparkSession, cfg: dict, source: str,
                   business_date: str) -> DataFrame:
    """Windowed canonical read. Returns canonical columns unchanged."""
    root = cfg["paths"]["canonical"]
    df = spark.read.format(storage_format(cfg)).load(f"{root}/{source}/")
    window_days = int(cfg["matching"]["settlement_window_days"])
    if source == "bank":
        return df.filter(
            (F.col("business_date") >= F.lit(business_date).cast("date")) &
            (F.col("business_date") <= F.date_add(
                F.lit(business_date).cast("date"), window_days)))
    return df.filter(F.col("business_date") == F.lit(business_date).cast("date"))


def read_recon_output(spark: SparkSession, cfg: dict,
                      business_date: str) -> DataFrame:
    path = cfg["paths"]["recon_output"]
    return (spark.read.format(storage_format(cfg)).load(path)
            .filter(F.col("business_date") == F.lit(business_date).cast("date")))
