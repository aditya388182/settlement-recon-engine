from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CANDIDATE_COLS = ["l_txn_uid", "r_txn_uid", "l_txn_ref", "r_txn_ref",
                  "block_key", "amount_diff", "fee_residual", "date_diff",
                  "score", "tier", "tolerance_applied"]


def _modelled_fee(fee_adjust: bool) -> "F.Column":
    """min(gross * rate_bps div 10000, max_fee_minor), integer arithmetic only.
    Zero when the counterparty also reports gross (the processor leg)."""
    if not fee_adjust:
        return F.lit(0).cast("bigint")
    return F.least(
        F.expr("(l.amount_minor * rate_bps) div 10000").cast("bigint"),
        F.col("max_fee_minor").cast("bigint"))


def _score() -> "F.Column":
    """1/(1+fee_residual) * 1/(1+date_diff). Monotone in both, bounded in (0, 1],
    a pure function of two integers, reproducible bit for bit."""
    return ((F.lit(1.0) / (F.lit(1.0) + F.col("fee_residual").cast("double")))
            * (F.lit(1.0) / (F.lit(1.0) + F.col("date_diff").cast("double"))))


def _common(joined: DataFrame, window_days: int, tier: str,
            fee_adjust: bool) -> DataFrame:
    return (joined
            .withColumn("amount_diff",
                        F.abs(F.col("l.amount_minor") - F.col("r.amount_minor")))
            .withColumn("_expected_net",
                        F.col("l.amount_minor") - _modelled_fee(fee_adjust))
            .withColumn("fee_residual",
                        F.abs(F.col("r.amount_minor") - F.col("_expected_net")))
            .withColumn("date_diff",
                        F.abs(F.datediff(F.col("l.business_date"),
                                         F.col("r.business_date"))))
            .filter(F.col("amount_diff") <= F.col("total_tolerance"))
            .filter(F.col("date_diff") <= F.lit(window_days))
            .withColumn("score", _score())
            .withColumn("tier", F.lit(tier))
            .select(F.col("l.txn_uid").alias("l_txn_uid"),
                    F.col("r.txn_uid").alias("r_txn_uid"),
                    F.col("l.txn_ref").alias("l_txn_ref"),
                    F.col("r.txn_ref").alias("r_txn_ref"),
                    F.col("l.block_key").alias("block_key"),
                    "amount_diff", "fee_residual", "date_diff", "score", "tier",
                    F.col("total_tolerance").alias("tolerance_applied")))


def _dedupe(candidates: DataFrame) -> DataFrame:
    """The dual-bucket rule can produce the same pair in two blocks. Collapse on
    the pair and keep the smallest block_key so the survivor is deterministic."""
    return (candidates
            .groupBy("l_txn_uid", "r_txn_uid", "l_txn_ref", "r_txn_ref", "tier")
            .agg(F.min("block_key").alias("block_key"),
                 F.min("amount_diff").alias("amount_diff"),
                 F.min("fee_residual").alias("fee_residual"),
                 F.min("date_diff").alias("date_diff"),
                 F.max("score").alias("score"),
                 F.min("tolerance_applied").alias("tolerance_applied"))
            .select(*CANDIDATE_COLS))


def generate_candidates(residual_l: DataFrame, residual_r: DataFrame,
                        tolerances: DataFrame, window_days: int,
                        ref_anchored: bool, fee_adjust: bool) -> DataFrame:
    """Blocked residual frames in, candidate pairs out.

    ref_anchored=True  -> tier 2, join on block_key AND txn_ref
    ref_anchored=False -> tier 3, join on block_key only
    """
    left = residual_l.join(F.broadcast(tolerances), "currency")
    on = [F.col("l.block_key") == F.col("r.block_key")]
    if ref_anchored:
        on.append(F.col("l.txn_ref") == F.col("r.txn_ref"))
    joined = left.alias("l").join(residual_r.alias("r"), on=on, how="inner")
    return _dedupe(_common(joined, window_days,
                           "T2" if ref_anchored else "T3", fee_adjust))


def candidate_density(candidates: DataFrame) -> DataFrame:
    """Per-block candidates / min(distinct left, distinct right).

    Measure this BEFORE trusting the ambiguity threshold. If ordinary blocks sit
    above it, the Hungarian fallback fires everywhere, the 'activates on exactly
    the seeded dense blocks' claim is false, and the cause is the candidate set
    rather than the threshold.
    """
    return (candidates.groupBy("block_key")
            .agg(F.count("*").alias("n_candidates"),
                 F.countDistinct("l_txn_uid").alias("n_l"),
                 F.countDistinct("r_txn_uid").alias("n_r"))
            .withColumn("density",
                        F.col("n_candidates")
                        / F.greatest(F.least("n_l", "n_r"), F.lit(1))))