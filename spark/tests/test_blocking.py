from datetime import date

from pyspark.sql import functions as F

from spark.recon import blocking


def _rows(spark, rows):
    return spark.createDataFrame(
        rows, "txn_uid string, business_date date, txn_ref string, "
              "amount_minor bigint, currency string")


def _keys(df, uid):
    return {r["block_key"] for r in df.filter(F.col("txn_uid") == uid).collect()}


def test_true_pair_survives_a_week_boundary(spark, cfg):
    """Saturday transaction, Monday settlement. Truncating to the week puts them
    in different buckets; the dual-bucket rule must still make them meet."""
    internal = _rows(spark, [("INT-1", date(2026, 7, 11), "M007-aaaa1111", 5000, "USD")])
    bank = _rows(spark, [("BNK-1", date(2026, 7, 13), "M007-aaaa1111", 4855, "USD")])
    bi = blocking.with_block_keys(internal, cfg)
    bb = blocking.with_block_keys(bank, cfg)
    assert _keys(bi, "INT-1") & _keys(bb, "BNK-1"), \
        "true pair split across blocks at the week boundary"


def test_same_week_pair_needs_no_second_bucket(spark, cfg):
    """Mid-week dates truncate to the same bucket, so only one key is emitted —
    the dual-bucket rule costs nothing when it is not needed."""
    internal = _rows(spark, [("INT-2", date(2026, 7, 7), "M001-bbbb2222", 5000, "USD")])
    assert len(_keys(blocking.with_block_keys(internal, cfg), "INT-2")) == 1


def test_boundary_row_emits_exactly_two_keys(spark, cfg):
    internal = _rows(spark, [("INT-3", date(2026, 7, 11), "M001-cccc3333", 5000, "USD")])
    assert len(_keys(blocking.with_block_keys(internal, cfg), "INT-3")) == 2


def test_block_key_is_currency_week_and_prefix(spark, cfg):
    internal = _rows(spark, [("INT-4", date(2026, 7, 7), "M009-dddd4444", 5000, "EUR")])
    key = next(iter(_keys(blocking.with_block_keys(internal, cfg), "INT-4")))
    ccy, week, prefix = key.split("|")
    assert ccy == "EUR"
    assert week == "2026-07-06"      # date_trunc('week') lands on Monday
    assert prefix == "M009"          # 4-char prefix IS the merchant code


def test_hot_counterparty_gets_the_longer_prefix(spark, cfg):
    hot_cfg = {**cfg, "matching": {**cfg["matching"], "hot_counterparties": ["M007"]}}
    rows = _rows(spark, [
        ("INT-5", date(2026, 7, 7), "M007-eeee5555", 5000, "USD"),
        ("INT-6", date(2026, 7, 7), "M008-ffff6666", 5000, "USD"),
    ])
    keyed = blocking.with_block_keys(rows, hot_cfg)
    assert next(iter(_keys(keyed, "INT-5"))).split("|")[2] == "M007-eee"   # 8 chars
    assert next(iter(_keys(keyed, "INT-6"))).split("|")[2] == "M008"       # 4 chars


def test_adaptive_prefix_cannot_split_a_true_pair(spark, cfg):
    """The safety property behind the Day-4 skew fix: a true pair shares the
    ENTIRE txn_ref, so no prefix length can separate it."""
    hot_cfg = {**cfg, "matching": {**cfg["matching"], "hot_counterparties": ["M007"]}}
    internal = _rows(spark, [("INT-7", date(2026, 7, 7), "M007-abcd7777", 5000, "USD")])
    bank = _rows(spark, [("BNK-7", date(2026, 7, 8), "M007-abcd7777", 4855, "USD")])
    bi = blocking.with_block_keys(internal, hot_cfg)
    bb = blocking.with_block_keys(bank, hot_cfg)
    assert _keys(bi, "INT-7") & _keys(bb, "BNK-7")


def test_block_stats_reports_the_largest_block(spark, cfg):
    rows = [(f"INT-{i}", date(2026, 7, 7), "M001-aaaa0000", 100, "USD")
            for i in range(7)]
    rows += [(f"INT-X{i}", date(2026, 7, 7), "M002-bbbb0000", 100, "USD")
             for i in range(2)]
    stats = blocking.block_stats(blocking.with_block_keys(_rows(spark, rows), cfg),
                                 cfg, "test")
    assert stats["n_blocks"] == 2
    assert stats["max_block"] == 7
    assert stats["n_rows"] == 9
