from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TemporalJoinError(Exception):
    """A gap or a miss in the reference-data intervals."""


def as_of_join(txn_df: DataFrame, ref_df: DataFrame,
               date_col: str = "business_date",
               assert_columns: list[str] | None = None) -> DataFrame:
    joined = txn_df.alias("t").join(
        ref_df.alias("r"),
        on=[F.col("t.currency") == F.col("r.currency"),
            F.col(f"t.{date_col}") >= F.col("r.valid_from"),
            F.col(f"t.{date_col}") < F.col("r.valid_to")],
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
