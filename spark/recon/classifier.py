from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MATCHED = "MATCHED"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
MISSING_IN_PROCESSOR = "MISSING_IN_PROCESSOR"
MISSING_IN_BANK = "MISSING_IN_BANK"
DUPLICATE = "DUPLICATE"
TIMING_DIFFERENCE = "TIMING_DIFFERENCE"

ALL_CLASSES = (MATCHED, AMOUNT_MISMATCH, MISSING_IN_PROCESSOR,
               MISSING_IN_BANK, DUPLICATE, TIMING_DIFFERENCE)

STATE_EXACT = "EXACT_MATCHED"
STATE_TOLERANT = "TOLERANT_MATCHED"
STATE_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
STATE_UNMATCHED = "UNMATCHED"

EVIDENCE_FIELDS = ["amount_diff", "fee_residual", "date_diff", "score",
                   "method", "tier", "hungarian_cost", "candidate_count",
                   "tolerance_applied"]


def classify(df: DataFrame) -> DataFrame:
    """Input must carry: leg, match_state, date_diff, counterpart_ref_exists.
    Returns the frame with a break_class column added."""
    matched = F.col("match_state").isin([STATE_EXACT, STATE_TOLERANT])
    date_diff = F.coalesce(F.col("date_diff"), F.lit(0))

    break_class = (
        F.when(F.col("match_state") == STATE_DUPLICATE_SUSPECT, F.lit(DUPLICATE))
        # 1. AMOUNT_MISMATCH before MISSING_* — a same-ref counterpart exists,
        #    so the money moved wrong rather than being absent.
        .when(~matched & F.col("counterpart_ref_exists"), F.lit(AMOUNT_MISMATCH))
        # 2. still unmatched and no counterpart ref anywhere: genuinely absent.
        .when(~matched & (F.col("leg") == "PROCESSOR"), F.lit(MISSING_IN_PROCESSOR))
        .when(~matched & (F.col("leg") == "BANK"), F.lit(MISSING_IN_BANK))
        # 3. matched, but the settlement landed on a later date.
        .when(matched & (date_diff > 0), F.lit(TIMING_DIFFERENCE))
        .otherwise(F.lit(MATCHED)))
    return df.withColumn("break_class", break_class)


def with_evidence(df: DataFrame) -> DataFrame:
    """Fold the flat decision columns into one struct and drop the loose ones.

    One column instead of nine keeps the published schema readable, and a struct
    survives schema evolution better than nine nullable columns that downstream
    readers have to know the names of.
    """
    return (df
            .withColumn("evidence", F.struct(*[F.col(c) for c in EVIDENCE_FIELDS]))
            .drop(*EVIDENCE_FIELDS))


def assert_evidence_complete(df: DataFrame) -> None:
    """No row that is not cleanly MATCHED may carry null evidence.

    A break without evidence is a number on a dashboard that nobody can action,
    which is worse than no number at all: it looks like information.
    """
    suspect = df.filter(
        (F.col("break_class") != MATCHED) & F.col("evidence").isNull())
    n = suspect.count()
    if n:
        suspect.select("source_system", "leg", "txn_uid", "break_class") \
               .show(20, truncate=False)
        raise ValueError(f"{n} non-MATCHED row(s) carry null evidence")

    # A matched row must name its counterpart; an unmatched one must not.
    bad_cp = df.filter(
        (F.col("match_state").isin([STATE_EXACT, STATE_TOLERANT]) &
         F.col("counterpart_txn_uid").isNull())
        | (F.col("match_state").isin([STATE_UNMATCHED, STATE_DUPLICATE_SUSPECT]) &
           F.col("counterpart_txn_uid").isNotNull()))
    n_bad = bad_cp.count()
    if n_bad:
        bad_cp.select("source_system", "leg", "txn_uid", "match_state",
                      "counterpart_txn_uid").show(20, truncate=False)
        raise ValueError(f"{n_bad} row(s) have a counterpart inconsistent with "
                         f"their match_state")
    print(f"evidence complete on all {df.count()} rows")
