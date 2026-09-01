#!/usr/bin/env python3
"""verify_day1.py — assert, do not eyeball.

The generator printed a control line (rows and sum(amount_minor) per currency
per source). The canonical Delta tables must reproduce it to the minor unit.
Any drift means canonicalization changed a VALUE, not just a representation,
and Day 1 has failed.

Also re-checks the answer-key inventory so a silently-regenerated fixture with
the wrong seed cannot pass.

    python scripts/verify_day1.py --date 2026-07-06

Exit code 0 only if every assertion passes. Non-zero is a hard Day-1 failure.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F                    # noqa: E402
from spark.common.session import build_spark, load_config  # noqa: E402

SOURCES = ("internal", "processor", "bank")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--fixtures", default="data/fixtures")
    a = p.parse_args(argv)

    cfg = load_config()
    canonical_root = cfg["paths"]["canonical"]
    with open(os.path.join(a.fixtures, f"control_line_{a.date}.json"),
              encoding="utf-8") as fh:
        control = json.load(fh)

    spark = build_spark(f"verify-day1-{a.date}", cfg)
    failures = []
    try:
        print(f"{'source':<11}{'ccy':<5}{'expected rows':>14}{'actual':>9}"
              f"{'expected sum':>18}{'actual sum':>18}  result")
        print("-" * 82)
        for src in SOURCES:
            df = spark.read.format("delta").load(f"{canonical_root}/{src}/")
            agg = {r["currency"]: (r["n"], r["s"]) for r in
                   df.groupBy("currency")
                     .agg(F.count("*").alias("n"),
                          F.sum("amount_minor").alias("s"))
                     .collect()}
            expected = control["sources"][src]
            for ccy in sorted(set(expected) | set(agg)):
                exp = expected.get(ccy, {"rows": 0, "sum_amount_minor": 0})
                act_n, act_s = agg.get(ccy, (0, 0))
                ok = (act_n == exp["rows"] and act_s == exp["sum_amount_minor"])
                if not ok:
                    failures.append(f"{src}/{ccy}: rows {act_n} vs {exp['rows']}, "
                                    f"sum {act_s} vs {exp['sum_amount_minor']}")
                print(f"{src:<11}{ccy:<5}{exp['rows']:>14}{act_n:>9}"
                      f"{exp['sum_amount_minor']:>18}{act_s:>18}  "
                      f"{'PASS' if ok else 'FAIL'}")

        # ---- no floats, no nulls, no lost precision -----------------------
        for src in SOURCES:
            df = spark.read.format("delta").load(f"{canonical_root}/{src}/")
            dtype = dict(df.dtypes)["amount_minor"]
            if dtype != "bigint":
                failures.append(f"{src}: amount_minor is {dtype}, expected bigint")
            nulls = df.filter("amount_minor is null or txn_ref is null "
                              "or business_date is null").count()
            if nulls:
                failures.append(f"{src}: {nulls} rows with null key/amount")

        # ---- answer-key inventory ----------------------------------------
        key = (spark.read.option("header", "true")
               .csv(os.path.join(a.fixtures, f"answer_key_{a.date}.csv")))
        print("-" * 82)
        key.groupBy("expected_class").count().orderBy("expected_class").show(truncate=False)
        n_hung = key.filter("expects_hungarian = 'true'").count()
        n_greedy_diff = key.filter("greedy_differs = 'true'").count()
        n_traps = key.filter("trap_group_id <> ''").select("trap_group_id").distinct().count()
        n_dense = key.filter("dense_block_id <> ''").select("dense_block_id").distinct().count()
        print(f"trap groups={n_traps}  dense blocks={n_dense}  "
              f"expects_hungarian rows={n_hung}  greedy_differs rows={n_greedy_diff}")
        if n_hung != n_dense * 16:
            failures.append(f"expects_hungarian rows {n_hung} != {n_dense * 16}")
        if n_greedy_diff != n_dense * 4:
            failures.append(f"greedy_differs rows {n_greedy_diff} != {n_dense * 4}")
    finally:
        spark.stop()

    print("=" * 82)
    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        print(f"DAY 1 VERIFICATION FAILED ({len(failures)} assertion(s))")
        return 1
    print("DAY 1 VERIFICATION PASSED — canonical tables reproduce the control "
          "line to the minor unit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
