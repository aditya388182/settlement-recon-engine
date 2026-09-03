from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.io import storage_format
from spark.recon.temporal import as_of_join


def derive_tolerances(spark: SparkSession, cfg: dict,
                      business_date: str) -> DataFrame:
    """Returns (currency, max_fee_minor, fx_rounding_minor, total_tolerance)
    as of business_date. One row per currency."""
    ref = spark.read.format(storage_format(cfg)).load(cfg["paths"]["reference_data"])
    epsilon = int(cfg["reference_data"]["epsilon_minor"])

    currencies = (ref.select("currency").distinct()
                  .withColumn("business_date", F.lit(business_date).cast("date")))

    fees = ref.filter("kind = 'FEE_SCHEDULE'").select(
        "currency", "max_fee_minor", "rate_bps", "fee_jitter_max",
        "valid_from", "valid_to")
    fx = ref.filter("kind = 'FX_PRECISION'").select(
        "currency", "fx_rounding_minor", "valid_from", "valid_to")

    with_fee = as_of_join(currencies, fees,
                          assert_columns=["max_fee_minor"]).drop("valid_from", "valid_to")
    with_fx = as_of_join(with_fee, fx,
                         assert_columns=["fx_rounding_minor"]).drop("valid_from", "valid_to")

    return (with_fx
            .withColumn("epsilon_minor", F.lit(epsilon).cast("bigint"))
            .withColumn("total_tolerance",
                        F.col("max_fee_minor") + F.col("fee_jitter_max")
                        + F.col("fx_rounding_minor") + F.col("epsilon_minor"))
            .select("currency", "max_fee_minor", "rate_bps", "fee_jitter_max",
                    "fx_rounding_minor", "epsilon_minor", "total_tolerance"))
