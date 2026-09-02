from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GRAIN = ["source_system", "leg", "txn_uid"]
AGG_KEYS = ["source_system", "leg", "currency"]


class ControlTotalViolation(Exception):
    """Raised before any write. Nothing is published when this is in flight."""


def assert_control_totals(input_ledger: DataFrame, output: DataFrame,
                          business_date: str) -> None:
    """input_ledger and output must both carry
    (source_system, leg, currency, txn_uid, amount_minor)."""

    dupes = output.groupBy(*GRAIN).agg(F.count("*").alias("n")).filter("n > 1")
    n_dupes = dupes.count()
    if n_dupes:
        print("CONTROL TOTALS — duplicated output grain (evidence):")
        dupes.orderBy(F.desc("n"), *GRAIN).show(20, truncate=False)
        raise ControlTotalViolation(
            f"output grain violated for {business_date}: {n_dupes} "
            f"(source_system, leg, txn_uid) keys appear more than once — "
            f"run aborted, nothing published")

    inp = (input_ledger.groupBy(*AGG_KEYS)
           .agg(F.sum("amount_minor").alias("in_sum"),
                F.count("*").alias("in_n")))
    out = (output.groupBy(*AGG_KEYS)
           .agg(F.sum("amount_minor").alias("out_sum"),
                F.count("*").alias("out_n")))
    diff = (inp.join(out, AGG_KEYS, "full_outer")
            .withColumn("in_sum", F.coalesce("in_sum", F.lit(0)))
            .withColumn("out_sum", F.coalesce("out_sum", F.lit(0)))
            .withColumn("in_n", F.coalesce("in_n", F.lit(0)))
            .withColumn("out_n", F.coalesce("out_n", F.lit(0)))
            .withColumn("delta_sum", F.col("out_sum") - F.col("in_sum"))
            .withColumn("delta_n", F.col("out_n") - F.col("in_n"))
            .filter("delta_sum <> 0 OR delta_n <> 0"))

    if diff.count() > 0:
        print("CONTROL TOTALS — money not conserved (evidence BEFORE abort):")
        diff.orderBy(*AGG_KEYS).show(50, truncate=False)
        first = diff.orderBy(*AGG_KEYS).first()
        raise ControlTotalViolation(
            f"money not conserved for {business_date} — "
            f"{first['source_system']}/{first['leg']}/{first['currency']} "
            f"delta_n={first['delta_n']} delta_sum={first['delta_sum']} minor — "
            f"run aborted, nothing published")

    total = (input_ledger.groupBy(*AGG_KEYS)
             .agg(F.count("*").alias("n"), F.sum("amount_minor").alias("s"))
             .orderBy(*AGG_KEYS))
    print(f"CONTROL TOTALS PASS — {business_date} — "
          f"counts and sums conserved per (source_system, leg, currency)")
    total.show(50, truncate=False)
