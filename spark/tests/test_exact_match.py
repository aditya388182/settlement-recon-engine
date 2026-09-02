from datetime import date

import pytest

from spark.recon import blocking, exact_match


def _rows(spark, rows):
    return spark.createDataFrame(
        rows, "txn_uid string, business_date date, txn_ref string, "
              "amount_minor bigint, currency string")


def test_duplicate_routing_keeps_the_first_uid(spark, cfg):
    df = _rows(spark, [
        ("PRC-0000002", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
        ("PRC-0000002-D1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
        ("PRC-0000003", date(2026, 7, 7), "M001-bbbb2222", 7000, "USD"),
    ])
    clean, suspects = exact_match.route_duplicate_suspects(df)
    assert sorted(r["txn_uid"] for r in clean.collect()) == \
        ["PRC-0000002", "PRC-0000003"]
    assert [r["txn_uid"] for r in suspects.collect()] == ["PRC-0000002-D1"]


def test_duplicate_routing_is_stable_under_input_order(spark, cfg):
    """Ordering by txn_uid, not by arrival order, is what makes 'which copy is
    the original' reproducible — and therefore part of byte-identity."""
    a = [("PRC-2", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
         ("PRC-2-D1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD")]
    first = exact_match.route_duplicate_suspects(_rows(spark, a))[1].collect()
    second = exact_match.route_duplicate_suspects(_rows(spark, list(reversed(a))))[1].collect()
    assert [r["txn_uid"] for r in first] == [r["txn_uid"] for r in second]


def test_exact_match_pairs_and_residuals(spark, cfg):
    internal = blocking.with_block_keys(_rows(spark, [
        ("INT-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
        ("INT-2", date(2026, 7, 7), "M001-bbbb2222", 7000, "USD"),
    ]), cfg)
    processor = blocking.with_block_keys(_rows(spark, [
        ("PRC-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
        ("PRC-2", date(2026, 7, 7), "M001-bbbb2222", 9999, "USD"),   # amount differs
    ]), cfg)
    pairs, l_res, r_res = exact_match.exact_match(internal, processor)
    assert pairs.count() == 1
    assert [r["txn_uid"] for r in l_res.collect()] == ["INT-2"]
    assert [r["txn_uid"] for r in r_res.collect()] == ["PRC-2"]


def test_dual_bucket_does_not_double_count_a_pair(spark, cfg):
    """A boundary-week row lives in two blocks. The pair must still appear once."""
    internal = blocking.with_block_keys(_rows(spark, [
        ("INT-1", date(2026, 7, 11), "M001-aaaa1111", 5000, "USD")]), cfg)
    processor = blocking.with_block_keys(_rows(spark, [
        ("PRC-1", date(2026, 7, 11), "M001-aaaa1111", 5000, "USD")]), cfg)
    assert internal.count() == 2 and processor.count() == 2
    assert exact_match.exact_match(internal, processor)[0].count() == 1


def test_unrouted_duplicates_fan_out_and_are_caught(spark, cfg):
    """Skip duplicate routing on purpose: the exact pass fans out and
    assert_one_to_one raises. This is the guard that turns a silent
    double-count into a named failure."""
    internal = blocking.with_block_keys(_rows(spark, [
        ("INT-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD")]), cfg)
    processor = blocking.with_block_keys(_rows(spark, [
        ("PRC-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
        ("PRC-1-D1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
    ]), cfg)
    pairs, _, _ = exact_match.exact_match(internal, processor)
    assert pairs.count() == 2
    with pytest.raises(ValueError, match="not one-to-one"):
        exact_match.assert_one_to_one(pairs, "leg PROCESSOR")


def test_routing_then_matching_is_one_to_one(spark, cfg):
    """The same input, routed first: exactly one pair, no exception."""
    raw = _rows(spark, [
        ("PRC-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
        ("PRC-1-D1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD"),
    ])
    clean, suspects = exact_match.route_duplicate_suspects(raw)
    internal = blocking.with_block_keys(_rows(spark, [
        ("INT-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD")]), cfg)
    pairs, _, _ = exact_match.exact_match(internal, blocking.with_block_keys(clean, cfg))
    assert pairs.count() == 1 and suspects.count() == 1
    exact_match.assert_one_to_one(pairs, "leg PROCESSOR")


def test_exact_match_requires_amount_equality(spark, cfg):
    """The bank leg reports NET, so it exact-matches nothing. That is the design,
    not a bug — it is why the tolerant pass exists."""
    internal = blocking.with_block_keys(_rows(spark, [
        ("INT-1", date(2026, 7, 7), "M001-aaaa1111", 5000, "USD")]), cfg)
    bank = blocking.with_block_keys(_rows(spark, [
        ("BNK-1", date(2026, 7, 7), "M001-aaaa1111", 4855, "USD")]), cfg)
    assert exact_match.exact_match(internal, bank)[0].count() == 0
