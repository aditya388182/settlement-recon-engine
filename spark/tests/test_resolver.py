import pandas as pd
import pytest

from spark.recon.resolver import (assert_one_to_one, greedy, hungarian,
                                  make_resolve_block, resolve_globally)

COLS = ["l_txn_uid", "r_txn_uid", "l_txn_ref", "r_txn_ref", "block_key",
        "amount_diff", "fee_residual", "date_diff", "score", "tier",
        "tolerance_applied"]


def _cands(rows):
    return pd.DataFrame(
        [(l, r, lr, rr, "USD|2026-07-06|M001", d, d, 0, 1.0 / (1 + d), "T3", 310)
         for l, r, lr, rr, d in rows], columns=COLS)


def test_greedy_is_deterministic_across_runs():
    pdf = _cands([("A", "x", "M001-a", "M001-p", 3),
                  ("A", "y", "M001-a", "M001-q", 1),
                  ("B", "y", "M001-b", "M001-q", 2)])
    first = greedy(pdf)[["l_txn_uid", "r_txn_uid"]].values.tolist()
    second = greedy(pdf.sample(frac=1, random_state=7))[["l_txn_uid", "r_txn_uid"]] \
        .values.tolist()
    assert first == second


def test_tie_break_uses_both_refs():
    """Two candidates with the SAME score. Ordering on l_txn_ref alone leaves
    (A,x) vs (A,y) ambiguous; the winner must be the lexicographically smaller
    r_txn_ref."""
    pdf = _cands([("A", "x", "M001-a", "M001-zzz", 5),
                  ("A", "y", "M001-a", "M001-aaa", 5)])
    out = greedy(pdf)
    assert out["r_txn_uid"].tolist() == ["y"]


def test_greedy_keeps_one_pair_per_side():
    pdf = _cands([("A", "x", "M001-a", "M001-p", 1),
                  ("A", "y", "M001-a", "M001-q", 2),
                  ("B", "x", "M001-b", "M001-p", 3)])
    out = greedy(pdf)
    assert len(out) == 1
    assert out["l_txn_uid"].nunique() == len(out)
    assert out["r_txn_uid"].nunique() == len(out)


def test_density_boundary_selects_the_method():
    """density = candidates / min(distinct l, distinct r). The threshold is a
    strict greater-than, so a block sitting exactly on it stays on greedy."""
    below = _cands([("A", "x", "M001-a", "M001-p", 1),
                    ("A", "y", "M001-a", "M001-q", 2),
                    ("B", "x", "M001-b", "M001-p", 3)])          # 3/2 = 1.5
    at = _cands([("A", "x", "M001-a", "M001-p", 1),
                 ("A", "y", "M001-a", "M001-q", 2),
                 ("B", "x", "M001-b", "M001-p", 3),
                 ("B", "y", "M001-b", "M001-q", 4)])             # 4/2 = 2.0
    above = _cands([("A", "x", "M001-a", "M001-p", 1),
                    ("A", "y", "M001-a", "M001-q", 2),
                    ("A", "z", "M001-a", "M001-r", 6),
                    ("B", "x", "M001-b", "M001-p", 3),
                    ("B", "y", "M001-b", "M001-q", 4)])          # 5/2 = 2.5
    fn = make_resolve_block(2.0)
    assert fn(below)["method"].unique().tolist() == ["GREEDY"]
    assert fn(at)["method"].unique().tolist() == ["GREEDY"]
    assert fn(above)["method"].unique().tolist() == ["HUNGARIAN"]


def test_greedy_and_hungarian_provably_differ():
    pdf = _cands([("A", "y", "M001-a", "M001-q", 1),   
                  ("A", "x", "M001-a", "M001-p", 5),   
                  ("B", "y", "M001-b", "M001-q", 6)]) 
    g = sorted(greedy(pdf)[["l_txn_uid", "r_txn_uid"]].itertuples(index=False, name=None))
    h = sorted(hungarian(pdf)[["l_txn_uid", "r_txn_uid"]].itertuples(index=False, name=None))
    assert g == [("A", "y")]
    assert h == [("A", "x"), ("B", "y")]
    assert g != h
    # cost of the optimal pair-set vs greedys with the prohibitive penalty
    two_pairs = (1 - 1 / 6) + (1 - 1 / 7)
    one_pair_plus_stranded = (1 - 1 / 2) + 10.0
    assert two_pairs < one_pair_plus_stranded


def test_hungarian_records_a_real_cost():
    pdf = _cands([("A", "x", "M001-a", "M001-p", 1),
                  ("A", "y", "M001-a", "M001-q", 2),
                  ("A", "z", "M001-a", "M001-r", 3),
                  ("B", "x", "M001-b", "M001-p", 4),
                  ("B", "y", "M001-b", "M001-q", 5)])
    out = hungarian(pdf)
    assert out["hungarian_cost"].notna().all()
    assert (out["hungarian_cost"] < 10.0).all()


def test_resolve_globally_breaks_cross_block_conflicts(spark):
    from spark.recon.resolver import ASSIGNMENT_SCHEMA
    rows = [
        ("A", "x", "M001-a", "M001-p", "blk1", 1, 1, 0, 0.50, "T3", 310,
         "GREEDY", None, 1, 1.0),
        ("A", "y", "M001-a", "M001-q", "blk2", 9, 9, 0, 0.10, "T3", 310,
         "GREEDY", None, 1, 1.0),
    ]
    df = spark.createDataFrame(rows, ASSIGNMENT_SCHEMA)
    out = resolve_globally(df)
    assert out.count() == 1
    assert out.collect()[0]["r_txn_uid"] == "x"     # the higher score survives
    assert_one_to_one(out, "test")
