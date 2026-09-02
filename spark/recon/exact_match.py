from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

STATE_EXACT = "EXACT_MATCHED"
STATE_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
STATE_PENDING = "UNRESOLVED_PENDING_TOLERANT"


def route_duplicate_suspects(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split a canonical source into (clean, duplicate_suspects).

    Returns rows at the ORIGINAL grain (one row per txn_uid), before blocking.
    """
    w = Window.partitionBy("txn_ref").orderBy(F.asc("txn_uid"))
    ranked = df.withColumn("_rn", F.row_number().over(w))
    clean = ranked.filter("_rn = 1").drop("_rn")
    suspects = ranked.filter("_rn > 1").drop("_rn")
    return clean, suspects


def exact_match(left: DataFrame, right: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Exact pass inside blocks.

    left/right are BLOCKED frames (one row per txn_uid x block_key).
    Join key: block_key + txn_ref + amount_minor. All three must agree; amount
    equality is what makes this an *exact* pass rather than a ref lookup.

    Returns (pairs, left_residual, right_residual) where

        pairs           one row per matched pair, deduped across the dual-bucket
                        explosion, columns l_txn_uid / r_txn_uid / l_txn_ref /
                        r_txn_ref / block_key
        left_residual   distinct left txn_uids with no match
        right_residual  distinct right txn_uids with no match

    Matched rows physically leave the pool. That is what keeps the Day-3 tolerant
    join small — pass 3 only ever sees residuals.
    """
    joined = (left.alias("l")
              .join(right.alias("r"),
                    on=[F.col("l.block_key") == F.col("r.block_key"),
                        F.col("l.txn_ref") == F.col("r.txn_ref"),
                        F.col("l.amount_minor") == F.col("r.amount_minor")],
                    how="inner")
              .select(F.col("l.txn_uid").alias("l_txn_uid"),
                      F.col("r.txn_uid").alias("r_txn_uid"),
                      F.col("l.txn_ref").alias("l_txn_ref"),
                      F.col("r.txn_ref").alias("r_txn_ref"),
                      F.col("l.block_key").alias("block_key")))

    # The dual-bucket rule can put a true pair in two blocks, producing the same
    # pair twice. Collapse on the pair, keeping the lexicographically smallest
    # block_key so the surviving row is deterministic.
    pairs = (joined.groupBy("l_txn_uid", "r_txn_uid", "l_txn_ref", "r_txn_ref")
             .agg(F.min("block_key").alias("block_key")))

    left_uids = left.select("txn_uid").distinct()
    right_uids = right.select("txn_uid").distinct()
    left_residual = left_uids.join(
        pairs.select(F.col("l_txn_uid").alias("txn_uid")).distinct(),
        on="txn_uid", how="left_anti")
    right_residual = right_uids.join(
        pairs.select(F.col("r_txn_uid").alias("txn_uid")).distinct(),
        on="txn_uid", how="left_anti")
    return pairs, left_residual, right_residual


def assert_one_to_one(pairs: DataFrame, label: str) -> None:
    """A pair set must be an injective mapping in both directions. If it is not,
    a fan-out slipped past duplicate routing and the control totals would fire
    later with a much less useful message than this one."""
    l_dupes = (pairs.groupBy("l_txn_uid").count().filter("count > 1"))
    r_dupes = (pairs.groupBy("r_txn_uid").count().filter("count > 1"))
    n_l, n_r = l_dupes.count(), r_dupes.count()
    if n_l or n_r:
        l_dupes.show(10, truncate=False)
        r_dupes.show(10, truncate=False)
        raise ValueError(
            f"{label}: exact pass is not one-to-one "
            f"({n_l} left uids and {n_r} right uids matched more than once)")
