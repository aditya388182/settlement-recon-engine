from datetime import date

import pytest

from spark.recon.temporal import TemporalJoinError, as_of_join

REF_SCHEMA = ("kind string, currency string, max_fee_minor bigint, "
              "rate_bps bigint, fee_jitter_max bigint, "
              "fx_rounding_minor bigint, version string, "
              "valid_from date, valid_to date")


def _ref(spark, rows):
    return spark.createDataFrame(rows, REF_SCHEMA)


def _write_ref(spark, tmp_path, rows):
    _ref(spark, rows).write.format("parquet").mode("overwrite").save(str(tmp_path))
    return str(tmp_path)


def _cfg_with(cfg, path):
    return {**cfg,
            "paths": {"reference_data": path},
            "storage": {"format": "parquet"}}


V1 = [
    ("FEE_SCHEDULE", "USD", 300, 290, 8, None, "v1", date(2026, 7, 1), date(9999, 12, 31)),
    ("FEE_SCHEDULE", "JPY", 400, 290, 8, None, "v1", date(2026, 7, 1), date(9999, 12, 31)),
    ("FX_PRECISION", "USD", None, None, None, 1, "v1", date(2026, 7, 1), date(9999, 12, 31)),
    ("FX_PRECISION", "JPY", None, None, None, 1, "v1", date(2026, 7, 1), date(9999, 12, 31)),
]


def test_tolerance_is_derived_not_hardcoded(spark, cfg, tmp_path):
    from spark.recon.tolerance import derive_tolerances
    path = _write_ref(spark, tmp_path / "ref1", V1)
    got = {r["currency"]: r["total_tolerance"]
           for r in derive_tolerances(spark, _cfg_with(cfg, path), "2026-07-06").collect()}
    assert got["USD"] == 300 + 8 + 1 + 1
    assert got["JPY"] == 400 + 8 + 1 + 1


def test_jpy_zero_decimal_tolerance_is_not_inflated(spark, cfg, tmp_path):
    from spark.recon.tolerance import derive_tolerances
    path = _write_ref(spark, tmp_path / "ref2", V1)
    got = {r["currency"]: r["total_tolerance"]
           for r in derive_tolerances(spark, _cfg_with(cfg, path), "2026-07-06").collect()}
    assert got["JPY"] < 1000, (
        "JPY tolerance looks scaled by decimal places — its minor unit IS the yen")


def test_as_of_join_picks_the_version_in_force(spark, cfg):
    rows = [
        ("FEE_SCHEDULE", "USD", 300, 290, 8, None, "v1",
         date(2026, 7, 1), date(2026, 7, 9)),
        ("FEE_SCHEDULE", "USD", 350, 290, 8, None, "v2",
         date(2026, 7, 9), date(9999, 12, 31)),
    ]
    ref = _ref(spark, rows).select("currency", "max_fee_minor", "version",
                                   "valid_from", "valid_to")
    txns = spark.createDataFrame(
        [("USD", date(2026, 7, 6)), ("USD", date(2026, 7, 10))],
        "currency string, business_date date")
    got = {r["business_date"].isoformat(): r["version"]
           for r in as_of_join(txns, ref, assert_columns=["max_fee_minor"]).collect()}
    assert got["2026-07-06"] == "v1"     # the schedule in force on that date
    assert got["2026-07-10"] == "v2"


def test_half_open_interval_excludes_the_end_date(spark, cfg):
    """[valid_from, valid_to). A transaction ON the changeover date belongs to
    the NEW version; if the interval were closed it would match both and the
    join would fan out."""
    rows = [
        ("FEE_SCHEDULE", "USD", 300, 290, 8, None, "v1",
         date(2026, 7, 1), date(2026, 7, 9)),
        ("FEE_SCHEDULE", "USD", 350, 290, 8, None, "v2",
         date(2026, 7, 9), date(9999, 12, 31)),
    ]
    ref = _ref(spark, rows).select("currency", "max_fee_minor", "version",
                                   "valid_from", "valid_to")
    txns = spark.createDataFrame([("USD", date(2026, 7, 9))],
                                 "currency string, business_date date")
    out = as_of_join(txns, ref, assert_columns=["max_fee_minor"]).collect()
    assert len(out) == 1 and out[0]["version"] == "v2"


def test_interval_gap_fails_loudly(spark, cfg):
    """A gap must surface at the join, not as mysteriously missing candidates
    three stages later."""
    rows = [
        ("FEE_SCHEDULE", "USD", 300, 290, 8, None, "v1",
         date(2026, 7, 1), date(2026, 7, 5)),
        ("FEE_SCHEDULE", "USD", 350, 290, 8, None, "v2",
         date(2026, 7, 9), date(9999, 12, 31)),
    ]
    ref = _ref(spark, rows).select("currency", "max_fee_minor", "version",
                                   "valid_from", "valid_to")
    txns = spark.createDataFrame([("USD", date(2026, 7, 6))],
                                 "currency string, business_date date")
    with pytest.raises(TemporalJoinError, match="gap"):
        as_of_join(txns, ref, assert_columns=["max_fee_minor"])
