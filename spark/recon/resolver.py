from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from scipy.optimize import linear_sum_assignment

PROHIBITIVE = 10.0

ASSIGNMENT_SCHEMA = T.StructType([
    T.StructField("l_txn_uid", T.StringType()),
    T.StructField("r_txn_uid", T.StringType()),
    T.StructField("l_txn_ref", T.StringType()),
    T.StructField("r_txn_ref", T.StringType()),
    T.StructField("block_key", T.StringType()),
    T.StructField("amount_diff", T.LongType()),
    T.StructField("fee_residual", T.LongType()),
    T.StructField("date_diff", T.IntegerType()),
    T.StructField("score", T.DoubleType()),
    T.StructField("tier", T.StringType()),
    T.StructField("tolerance_applied", T.LongType()),
    T.StructField("method", T.StringType()),
    T.StructField("hungarian_cost", T.DoubleType()),
    T.StructField("candidate_count", T.IntegerType()),
    T.StructField("block_density", T.DoubleType()),
])
_OUT_COLS = [f.name for f in ASSIGNMENT_SCHEMA.fields]


_PANDAS_DTYPES = {
    "l_txn_uid": "object", "r_txn_uid": "object", "l_txn_ref": "object",
    "r_txn_ref": "object", "block_key": "object", "amount_diff": "int64",
    "fee_residual": "int64", "date_diff": "int32", "score": "float64", "tier": "object",
    "tolerance_applied": "int64", "method": "object",
    "hungarian_cost": "float64", "candidate_count": "int32",
    "block_density": "float64",
}


def _empty() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=d) for c, d in _PANDAS_DTYPES.items()})


def _coerce(out: pd.DataFrame) -> pd.DataFrame:
    """Arrow will not coerce for you. An int64 column against an IntegerType
    field kills the Python worker with a bare EOFError and no useful message,
    so the UDF pins every dtype on the way out."""
    out = out[_OUT_COLS].copy()
    for c, d in _PANDAS_DTYPES.items():
        if d == "object":
            out[c] = out[c].astype("object")
        else:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype(d)
    return out


def greedy(pdf: pd.DataFrame) -> pd.DataFrame:
    """Score descending, then BOTH refs ascending, stable sort. Keep a pair only
    if neither side is already taken."""
    ordered = pdf.sort_values(["score", "l_txn_ref", "r_txn_ref"],
                              ascending=[False, True, True], kind="mergesort")
    taken_l: set = set()
    taken_r: set = set()
    keep = []
    for row in ordered.itertuples(index=False):
        if row.l_txn_uid in taken_l or row.r_txn_uid in taken_r:
            continue
        taken_l.add(row.l_txn_uid)
        taken_r.add(row.r_txn_uid)
        keep.append(row)
    out = pd.DataFrame(keep, columns=pdf.columns) if keep else pdf.iloc[0:0].copy()
    out["method"] = "GREEDY"
    out["hungarian_cost"] = np.nan
    return out


def hungarian(pdf: pd.DataFrame) -> pd.DataFrame:
    """Globally optimal assignment on the block, via linear_sum_assignment."""
    ls = sorted(pdf["l_txn_uid"].unique())
    rs = sorted(pdf["r_txn_uid"].unique())
    li = {u: i for i, u in enumerate(ls)}
    ri = {u: i for i, u in enumerate(rs)}
    cost = np.full((len(ls), len(rs)), PROHIBITIVE)
    for row in pdf.itertuples(index=False):
        cost[li[row.l_txn_uid], ri[row.r_txn_uid]] = 1.0 - float(row.score)
    rows, cols = linear_sum_assignment(cost)

    kept, costs = [], []
    for i, j in zip(rows, cols):
        if cost[i, j] >= PROHIBITIVE:
            continue                      # a non-candidate cell: not a match
        kept.append((ls[i], rs[j]))
        costs.append(float(cost[i, j]))
    if not kept:
        out = pdf.iloc[0:0].copy()
        out["method"] = "HUNGARIAN"
        out["hungarian_cost"] = np.nan
        return out

    keep_df = pd.DataFrame(kept, columns=["l_txn_uid", "r_txn_uid"])
    keep_df["hungarian_cost"] = costs
    out = pdf.merge(keep_df, on=["l_txn_uid", "r_txn_uid"], how="inner")
    if (out["hungarian_cost"] >= PROHIBITIVE).any():
        raise ValueError("Hungarian kept a prohibitive-cost pair — the block was "
                         "over-split; a candidate edge is missing from this block")
    out["method"] = "HUNGARIAN"
    return out


def make_resolve_block(threshold: float, force_greedy: bool = False):
    """Returns the applyInPandas function. threshold is captured by value so the
    worker never reads config."""
    def resolve_block(pdf: pd.DataFrame) -> pd.DataFrame:
        if pdf.empty:
            return _empty()
        n_l = pdf["l_txn_uid"].nunique()
        n_r = pdf["r_txn_uid"].nunique()
        density = len(pdf) / max(min(n_l, n_r), 1)
        counts = pdf.groupby("l_txn_uid")["r_txn_uid"].transform("count")
        pdf = pdf.assign(candidate_count=counts.astype("int32"),
                         block_density=float(density))
        if force_greedy or not density > threshold:
            out = greedy(pdf)
        else:
            print(f"HUNGARIAN activated block={pdf['block_key'].iloc[0]} "
                  f"density={density:.3f} pairs={len(pdf)}", flush=True)
            out = hungarian(pdf)
        for c in _OUT_COLS:
            if c not in out.columns:
                out[c] = None
        return _coerce(out)
    return resolve_block


def resolve(candidates: DataFrame, cfg: dict,
            force_greedy: bool = False) -> DataFrame:
    """Per-block resolution followed by the global cross-block reduce."""
    threshold = float(cfg["matching"]["ambiguity_density_threshold"])
    fn = make_resolve_block(threshold, force_greedy)
    per_block = candidates.groupBy("block_key").applyInPandas(fn, ASSIGNMENT_SCHEMA)
    return resolve_globally(per_block)


def resolve_globally(assignments: DataFrame) -> DataFrame:
    """Enforce one-to-one across block boundaries, deterministically.

    Same ordering rule as greedy: score desc, then both refs. Applied on the
    left side first, then the right, then asserted.
    """
    order = [F.desc("score"), F.asc("l_txn_ref"), F.asc("r_txn_ref"),
             F.asc("l_txn_uid"), F.asc("r_txn_uid")]
    keep_l = (assignments
              .withColumn("_rn", F.row_number().over(
                  Window.partitionBy("l_txn_uid").orderBy(*order)))
              .filter("_rn = 1").drop("_rn"))
    keep = (keep_l
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("r_txn_uid").orderBy(*order)))
            .filter("_rn = 1").drop("_rn"))
    return keep


def assert_one_to_one(assignments: DataFrame, label: str) -> None:
    for side in ("l_txn_uid", "r_txn_uid"):
        dupes = assignments.groupBy(side).count().filter("count > 1")
        n = dupes.count()
        if n:
            dupes.show(10, truncate=False)
            raise ValueError(f"{label}: resolution is not one-to-one — "
                             f"{n} {side} value(s) assigned more than once")
