from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.io import storage_format

OUTPUT_COLS = [
    "business_date",        # run date == partition key
    "source_system",
    "leg",
    "txn_uid",
    "txn_ref",
    "amount_minor",
    "currency",
    "row_business_date",    # the source row's own date
    "match_state",          # EXACT_MATCHED | TOLERANT_MATCHED | DUPLICATE_SUSPECT | UNMATCHED
    "break_class",          # the six-class verdict
    "counterpart_txn_uid",
    "counterpart_ref",
    "evidence",             # struct: diffs, score, method, tier, cost, contest, tolerance
    "block_key",
    "run_ts",
]

SUMMARY_COLS = ["business_date", "leg", "break_class", "currency", "n",
                "sum_amount_minor"]


def _write_partition(df: DataFrame, path: str, fmt: str, business_date: str,
                     label: str) -> None:
    writer = df.write.format(fmt).mode("overwrite").partitionBy("business_date")
    if fmt == "delta":
        writer = writer.option("replaceWhere", f"business_date = '{business_date}'")
    else:
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode",
                                 "dynamic")
        print(f"[output] format={fmt}: using dynamic partition overwrite. "
              f"replaceWhere is the delta-only mechanism this project claims.")
    writer.save(path)
    print(f"wrote {label} partition business_date={business_date} -> {path}")


def write_recon_output(df: DataFrame, cfg: dict, business_date: str) -> None:
    _write_partition(df.select(*OUTPUT_COLS), cfg["paths"]["recon_output"],
                     storage_format(cfg), business_date, "recon_output")


def build_summary(df: DataFrame) -> DataFrame:
    """Feeds the Grafana break-trend dashboards on Day 6."""
    return (df.groupBy("business_date", "leg", "break_class", "currency")
            .agg(F.count("*").alias("n"),
                 F.sum("amount_minor").alias("sum_amount_minor"))
            .select(*SUMMARY_COLS))


def write_summary(df: DataFrame, cfg: dict, business_date: str) -> DataFrame:
    summary = build_summary(df).cache()
    _write_partition(summary, cfg["paths"]["recon_summary"],
                     storage_format(cfg), business_date, "recon_summary")
    return summary
