import pytest

from spark.recon.classifier import (AMOUNT_MISMATCH, DUPLICATE, MATCHED,
                                    MISSING_IN_BANK, MISSING_IN_PROCESSOR,
                                    TIMING_DIFFERENCE, assert_evidence_complete,
                                    classify, with_evidence)

SCHEMA = ("txn_uid string, source_system string, leg string, match_state string, "
          "date_diff int, counterpart_ref_exists boolean, "
          "counterpart_txn_uid string, amount_diff bigint, fee_residual bigint, "
          "score double, method string, tier string, hungarian_cost double, "
          "candidate_count int, tolerance_applied bigint")


def _row(uid, leg, state, date_diff=0, ref_exists=False, cp=None):
    return (uid, "INTERNAL", leg, state, date_diff, ref_exists, cp,
            0, 0, 1.0, "EXACT", "T1", None, 1, 310)


def _classified(spark, rows):
    df = classify(spark.createDataFrame(rows, SCHEMA))
    return {r["txn_uid"]: r["break_class"] for r in df.collect()}


def test_one_case_per_class(spark):
    got = _classified(spark, [
        _row("a", "PROCESSOR", "EXACT_MATCHED", cp="PRC-1"),
        _row("b", "BANK", "TOLERANT_MATCHED", date_diff=2, cp="BNK-1"),
        _row("c", "PROCESSOR", "DUPLICATE_SUSPECT"),
        _row("d", "PROCESSOR", "UNMATCHED", ref_exists=True),
        _row("e", "PROCESSOR", "UNMATCHED", ref_exists=False),
        _row("f", "BANK", "UNMATCHED", ref_exists=False),
    ])
    assert got == {"a": MATCHED, "b": TIMING_DIFFERENCE, "c": DUPLICATE,
                   "d": AMOUNT_MISMATCH, "e": MISSING_IN_PROCESSOR,
                   "f": MISSING_IN_BANK}


def test_amount_mismatch_is_decided_before_missing(spark):
    """Same input except for counterpart_ref_exists. A counterpart carrying the
    same ref makes it a mismatch; no counterpart makes it missing."""
    got = _classified(spark, [
        _row("with_ref", "PROCESSOR", "UNMATCHED", ref_exists=True),
        _row("no_ref", "PROCESSOR", "UNMATCHED", ref_exists=False),
    ])
    assert got["with_ref"] == AMOUNT_MISMATCH
    assert got["no_ref"] == MISSING_IN_PROCESSOR


def test_duplicate_wins_over_everything(spark):
    """A duplicate-suspect row never entered matching, so no other rule may
    claim it — not even when its ref exists on the other side."""
    got = _classified(spark, [
        _row("d", "PROCESSOR", "DUPLICATE_SUSPECT", ref_exists=True)])
    assert got["d"] == DUPLICATE


def test_matched_with_zero_date_diff_is_not_a_timing_difference(spark):
    got = _classified(spark, [
        _row("m", "BANK", "TOLERANT_MATCHED", date_diff=0, cp="BNK-1")])
    assert got["m"] == MATCHED


def test_null_date_diff_is_treated_as_zero(spark):
    rows = [(("n",) + ("INTERNAL", "BANK", "TOLERANT_MATCHED", None, False,
                       "BNK-1", 0, 0, 1.0, "EXACT", "T1", None, 1, 310))]
    got = {r["txn_uid"]: r["break_class"]
           for r in classify(spark.createDataFrame(rows, SCHEMA)).collect()}
    assert got["n"] == MATCHED


def test_evidence_struct_replaces_the_flat_columns(spark):
    df = with_evidence(classify(spark.createDataFrame(
        [_row("a", "PROCESSOR", "UNMATCHED", ref_exists=True)], SCHEMA)))
    cols = set(df.columns)
    assert "evidence" in cols
    assert not ({"amount_diff", "score", "method", "tier"} & cols)
    ev = df.collect()[0]["evidence"]
    assert ev["tolerance_applied"] == 310 and ev["method"] == "EXACT"


def test_evidence_completeness_catches_a_counterpart_inconsistency(spark):
    """An unmatched row must not name a counterpart, and a matched row must."""
    df = with_evidence(classify(spark.createDataFrame(
        [_row("bad", "PROCESSOR", "UNMATCHED", ref_exists=True, cp="PRC-9")],
        SCHEMA)))
    with pytest.raises(ValueError, match="inconsistent"):
        assert_evidence_complete(df)


def test_evidence_completeness_passes_on_a_consistent_frame(spark):
    df = with_evidence(classify(spark.createDataFrame([
        _row("a", "PROCESSOR", "EXACT_MATCHED", cp="PRC-1"),
        _row("b", "PROCESSOR", "UNMATCHED", ref_exists=True),
    ], SCHEMA)))
    assert_evidence_complete(df)
