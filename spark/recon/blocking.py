from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BLOCK_COLS = ["txn_uid", "block_key"]


def _prefix_col(cfg: dict):
    """Deterministic per-row prefix length. Hot counterparties are identified by
    the first 4 chars of txn_ref, which ARE the merchant code by construction."""
    m = cfg["matching"]
    default_len = int(m["block_prefix_len_default"])
    hot_len = int(m["block_prefix_len_hot"])
    hot = list(m.get("hot_counterparties") or [])
    short = F.substring(F.col("txn_ref"), 1, default_len)
    if not hot:
        return short
    long = F.substring(F.col("txn_ref"), 1, hot_len)
    return F.when(F.substring(F.col("txn_ref"), 1, 4).isin(hot), long).otherwise(short)


def _week(col) -> "F.Column":
    return F.date_trunc("week", col).cast("date").cast("string")


def with_block_keys(df: DataFrame, cfg: dict) -> DataFrame:
    """Explode each row into up to two blocks. Output grain is
    (all original columns, block_key) with one row per (txn_uid, block_key)."""
    window_days = int(cfg["matching"]["settlement_window_days"])
    prefix = _prefix_col(cfg)
    w1 = _week(F.col("business_date"))
    w2 = _week(F.date_add(F.col("business_date"), window_days))
    keyed = (df
             .withColumn("_prefix", prefix)
             .withColumn("_keys", F.array_distinct(F.array(
                 F.concat_ws("|", F.col("currency"), w1, F.col("_prefix")),
                 F.concat_ws("|", F.col("currency"), w2, F.col("_prefix")))))
             .withColumn("block_key", F.explode("_keys"))
             .drop("_keys", "_prefix"))
    return keyed


def block_stats(blocked: DataFrame, cfg: dict, label: str) -> Dict[str, int]:
    """Emitted every run. The max_block WARN is the Day-4 salt-list trigger and
    the first line of the overran_sla runbook."""
    stats = (blocked.groupBy("block_key").agg(F.count("*").alias("n"))
             .selectExpr("max(n) as max_block",
                         "percentile_approx(n, 0.99) as p99_block",
                         "count(*) as n_blocks",
                         "sum(n) as n_rows")
             .collect()[0].asDict())
    out = {k: int(v) if v is not None else 0 for k, v in stats.items()}
    print(f"[block_stats:{label}] blocks={out['n_blocks']} rows={out['n_rows']} "
          f"max_block={out['max_block']} p99={out['p99_block']}")
    if out["max_block"] > 5000:
        worst = (blocked.groupBy("block_key").agg(F.count("*").alias("n"))
                 .orderBy(F.desc("n"), F.asc("block_key")).limit(3).collect())
        for r in worst:
            print(f"[WARN] oversized block {r['block_key']} n={r['n']} — "
                  f"candidate for matching.hot_counterparties")
    return out
