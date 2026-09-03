#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row                                          # noqa: E402
from spark.common.io import storage_format                           # noqa: E402
from spark.common.session import (DEFAULT_CONFIG, build_spark,       # noqa: E402
                                  load_config)

FAR_FUTURE = "9999-12-31"
SCHEMA = ("kind string, currency string, max_fee_minor bigint, rate_bps bigint, "
          "fee_jitter_max bigint, fx_rounding_minor bigint, version string, "
          "valid_from date, valid_to date")


def _rows(cfg: dict, version: str, valid_from: str, valid_to: str,
          fee_bump: int = 0) -> list[Row]:
    rd = cfg["reference_data"]
    out = []
    for ccy, fee in sorted(rd["fee_schedule"].items()):
        out.append(Row(kind="FEE_SCHEDULE", currency=ccy,
                       max_fee_minor=int(fee["max_fee_minor"]) + fee_bump,
                       rate_bps=int(fee["rate_bps"]),
                       fee_jitter_max=int(rd.get("fee_jitter_max", 0)),
                       fx_rounding_minor=None, version=version,
                       valid_from=date.fromisoformat(valid_from),
                       valid_to=date.fromisoformat(valid_to)))
    for ccy, fx in sorted(rd["fx_precision"].items()):
        # JPY is the zero-decimal trap. fx_rounding_minor is expressed in MINOR
        # units already, so it does not scale with the number of decimals. A
        # formula that multiplies by 10**decimals inflates JPY's tolerance 100x
        # and silently swallows real breaks.
        out.append(Row(kind="FX_PRECISION", currency=ccy,
                       max_fee_minor=None, rate_bps=None, fee_jitter_max=None,
                       fx_rounding_minor=int(fx["fx_rounding_minor"]),
                       version=version,
                       valid_from=date.fromisoformat(valid_from),
                       valid_to=date.fromisoformat(valid_to)))
    return out


def assert_intervals(rows: list[Row]) -> None:
    by_key: dict[tuple[str, str], list[Row]] = {}
    for r in rows:
        by_key.setdefault((r["kind"], r["currency"]), []).append(r)
    for (kind, ccy), rs in sorted(by_key.items()):
        rs = sorted(rs, key=lambda x: x["valid_from"])
        for a, b in zip(rs, rs[1:]):
            if a["valid_to"] > b["valid_from"]:
                raise ValueError(f"{kind}/{ccy}: intervals overlap at "
                                 f"{a['valid_to']} > {b['valid_from']}")
            if a["valid_to"] < b["valid_from"]:
                raise ValueError(f"{kind}/{ccy}: interval gap between "
                                 f"{a['valid_to']} and {b['valid_from']}")
    print(f"interval invariant OK — {len(by_key)} (kind, currency) series, "
          f"non-overlapping and gapless")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--version", default="v1", choices=("v1", "v2"))
    p.add_argument("--effective", default="2026-07-09",
                   help="v2 only: the date the new schedule takes force")
    p.add_argument("--fee-bump", type=int, default=50,
                   help="v2 only: minor units added to max_fee_minor")
    p.add_argument("--config", default=DEFAULT_CONFIG)
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    path = cfg["paths"]["reference_data"]
    spark = build_spark(f"seed-reference-{a.version}", cfg)
    try:
        if a.version == "v1":
            rows = _rows(cfg, "v1", cfg["reference_data"]["valid_from"], FAR_FUTURE)
        else:
            # NEVER update in place. Close v1's interval, open v2's.
            existing = spark.read.format(storage_format(cfg)).load(path)
            rows = [Row(**{**r.asDict(),
                           "valid_to": date.fromisoformat(a.effective)})
                    for r in existing.filter("version = 'v1'").collect()]
            rows += _rows(cfg, "v2", a.effective, FAR_FUTURE, fee_bump=a.fee_bump)

        assert_intervals(rows)
        df = spark.createDataFrame(rows, schema=SCHEMA)
        df.write.format(storage_format(cfg)).mode("overwrite").save(path)
        print(f"wrote {df.count()} reference rows ({a.version}) -> {path}")
        df.orderBy("kind", "currency", "valid_from").show(50, truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
