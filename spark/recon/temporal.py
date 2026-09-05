from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TemporalJoinError(Exception):
    """A gap or a miss in the reference-data intervals."""


def as_of_join(txn_df: DataFrame, ref_df: DataFrame,
               date_col: str = "business_date",
               assert_columns: list[str] | None = None,
               point_in_time: bool = True) -> DataFrame:
    """point_in_time=False drops the date predicate entirely and joins on
    currency alone — the negative control's "always use today's fees".

    Worth noticing that the naive version cannot simply reuse this function with
    the current rows filtered in: the interval assertion catches it first,
    because a v2 row effective 9 July does not cover a 6 July transaction. You
    have to deliberately remove the date predicate to make the wrong thing run
    at all, which is itself a small piece of evidence that the mechanism is
    load-bearing rather than decorative.
    """
    on = [F.col("t.currency") == F.col("r.currency")]
    if point_in_time:
        on += [F.col(f"t.{date_col}") >= F.col("r.valid_from"),
               F.col(f"t.{date_col}") < F.col("r.valid_to")]
    joined = txn_df.alias("t").join(ref_df.alias("r"), on=on,
                                    how="left").drop(F.col("r.currency"))

    for col in (assert_columns or []):
        n_null = joined.filter(F.col(col).isNull()).count()
        if n_null:
            (joined.filter(F.col(col).isNull())
             .select("currency", date_col).distinct()
             .show(20, truncate=False))
            raise TemporalJoinError(
                f"AS OF join produced {n_null} row(s) with null {col!r} — "
                f"a gap in the [valid_from, valid_to) intervals. "
                f"Reference data is incomplete for the currencies and dates above.")
    return joined
