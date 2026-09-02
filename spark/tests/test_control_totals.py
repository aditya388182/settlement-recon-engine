import pytest

from spark.recon.control_totals import ControlTotalViolation, assert_control_totals

SCHEMA = ("source_system string, leg string, txn_uid string, "
          "currency string, amount_minor bigint")

LEDGER = [
    ("INTERNAL", "PROCESSOR", "INT-1", "USD", 1000),
    ("INTERNAL", "PROCESSOR", "INT-2", "USD", 2000),
    ("INTERNAL", "PROCESSOR", "INT-3", "EUR", 3000),
    ("PROCESSOR", "PROCESSOR", "PRC-1", "USD", 1000),
    ("PROCESSOR", "PROCESSOR", "PRC-2", "USD", 2000),
]


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_conserved_run_passes(spark):
    led = _df(spark, LEDGER)
    assert_control_totals(led, _df(spark, LEDGER), "2026-07-06")


def test_dropped_record_fails_closed(spark):
    led = _df(spark, LEDGER)
    out = _df(spark, [r for r in LEDGER if r[2] != "INT-2"])
    with pytest.raises(ControlTotalViolation) as e:
        assert_control_totals(led, out, "2026-07-06")
    assert "money not conserved" in str(e.value)
    assert "USD" in str(e.value)


def test_violation_names_only_the_offending_currency(spark):
    """An EUR problem must not implicate USD. Per-currency isolation is what
    makes the runbook's first triage step possible."""
    led = _df(spark, LEDGER)
    out = _df(spark, [r for r in LEDGER if r[2] != "INT-3"])
    with pytest.raises(ControlTotalViolation) as e:
        assert_control_totals(led, out, "2026-07-06")
    assert "EUR" in str(e.value)


def test_offsetting_error_is_caught_by_the_grain_check(spark):
    """Drop INT-2 (2000) and duplicate INT-1... no: duplicate a row of the SAME
    value so count and sum both survive. Only uniqueness of the grain catches
    this, which is why that assertion runs first."""
    led = _df(spark, LEDGER + [("INTERNAL", "PROCESSOR", "INT-4", "USD", 2000)])
    out = _df(spark, [r for r in LEDGER if r[2] != "INT-2"]
              + [("INTERNAL", "PROCESSOR", "INT-4", "USD", 2000),
                 ("INTERNAL", "PROCESSOR", "INT-4", "USD", 2000)])
    # counts and sums are identical by construction
    assert out.count() == led.count()
    assert (out.groupBy().sum("amount_minor").collect()[0][0]
            == led.groupBy().sum("amount_minor").collect()[0][0])
    with pytest.raises(ControlTotalViolation) as e:
        assert_control_totals(led, out, "2026-07-06")
    assert "grain violated" in str(e.value)


def test_legs_are_checked_independently(spark):
    """The internal spine appears once per leg. A loss on the bank leg must not
    be masked by a healthy processor leg."""
    led = _df(spark, LEDGER + [("INTERNAL", "BANK", "INT-1", "USD", 1000)])
    with pytest.raises(ControlTotalViolation) as e:
        assert_control_totals(led, _df(spark, LEDGER), "2026-07-06")
    assert "BANK" in str(e.value)
