from __future__ import annotations

from pyspark.sql import DataFrame

from spark.common.io import storage_format

OUTPUT_COLS = [
    "business_date", "source_system", "leg", "txn_uid", "txn_ref",
    "amount_minor", "currency", "row_business_date", "match_state",
    "counterpart_txn_uid", "counterpart_ref",
    "amount_diff", "fee_residual", "date_diff", "score",
    "method", "tier", "hungarian_cost", "candidate_count",
    "tolerance_applied", "block_key", "run_ts",
]


def write_recon_output(df: DataFrame, cfg: dict, business_date: str) -> None:
    path = cfg["paths"]["recon_output"]
    (df.select(*OUTPUT_COLS)
       .write.format(storage_format(cfg))
       .mode("overwrite")
       .partitionBy("business_date")
       .save(path))
    print(f"wrote recon_output partition business_date={business_date} -> {path}")
