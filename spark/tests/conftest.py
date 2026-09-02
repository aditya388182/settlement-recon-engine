import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .appName("recon-tests")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def cfg():
    """Minimal config slice the matching modules read."""
    return {
        "matching": {
            "settlement_window_days": 2,
            "ambiguity_density_threshold": 2.0,
            "block_prefix_len_default": 4,
            "block_prefix_len_hot": 8,
            "hot_counterparties": [],
        },
        "reference_data": {
            "epsilon_minor": 1,
            "fee_schedule": {
                "USD": {"rate_bps": 290, "max_fee_minor": 300},
                "JPY": {"rate_bps": 290, "max_fee_minor": 400},
            },
            "fx_precision": {
                "USD": {"decimals": 2, "fx_rounding_minor": 1},
                "JPY": {"decimals": 0, "fx_rounding_minor": 1},
            },
        },
    }
