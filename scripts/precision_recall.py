#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F                                
from spark.common.io import read_recon_output                         
from spark.common.session import (DEFAULT_CONFIG, build_spark,        # noqa: E402 above imports
                                  load_config)

JOIN_KEYS = ["source_system", "txn_uid", "leg"]
STATE_EXACT = "EXACT_MATCHED"
STATE_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"


class Result:
    def __init__(self):
        self.rows = []
        self.failed = False

    def add(self, check: str, detail: str, ok: bool, n: int = 0):
        self.rows.append((check, detail, "PASS" if ok else "FAIL", n))
        if not ok:
            self.failed = True

    def render(self) -> None:
        w = max(len(r[1]) for r in self.rows) + 2
        print()
        print(f"{'CHECK':<6}{'ASSERTION':<{w}}{'RESULT':<8}{'OFFENDING':>10}")
        print("-" * (6 + w + 18))
        for check, detail, res, n in self.rows:
            print(f"{check:<6}{detail:<{w}}{res:<8}{n:>10}")
        print("-" * (6 + w + 18))


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--fixtures", default="data/fixtures")
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    spark = build_spark(f"precision-recall-{a.date}", cfg)
    r = Result()
    try:
        out = read_recon_output(spark, cfg, a.date).cache()
        key = (spark.read.option("header", "true")
               .csv(os.path.join(a.fixtures, f"answer_key_{a.date}.csv"))
               .cache())

        only_key = key.join(out, JOIN_KEYS, "left_anti")
        only_out = out.join(key, JOIN_KEYS, "left_anti")
        n_ok, n_oo = only_key.count(), only_out.count()
        if n_ok:
            print("rows in the answer key with no output row:")
            only_key.select(*JOIN_KEYS, "expected_class").show(20, truncate=False)
        if n_oo:
            print("rows in the output with no answer-key row:")
            only_out.select(*JOIN_KEYS, "match_state").show(20, truncate=False)
        r.add("C1", "answer key rows without an output row", n_ok == 0, n_ok)
        r.add("C1", "output rows without an answer key row", n_oo == 0, n_oo)

        j = key.join(out, JOIN_KEYS, "inner").cache()

        want = j.filter((F.col("leg") == "PROCESSOR") &
                        (F.col("expected_class") == "MATCHED"))
        missed = want.filter(F.col("match_state") != STATE_EXACT)
        n_want, n_missed = want.count(), missed.count()
        if n_missed:
            missed.select(*JOIN_KEYS, "match_state").show(20, truncate=False)
        recall = 0.0 if n_want == 0 else (n_want - n_missed) / n_want
        r.add("C2", f"exact recall on leg PROCESSOR ({recall:.4%} of {n_want})",
              n_missed == 0, n_missed)

        wrong = j.filter((F.col("match_state") == STATE_EXACT) &
                         (F.col("expected_class") != "MATCHED"))
        n_wrong = wrong.count()
        if n_wrong:
            print("EXACT_MATCHED rows the answer key does not call MATCHED:")
            wrong.groupBy("expected_class").count().show(truncate=False)
        r.add("C3", "exact precision (false exact matches)", n_wrong == 0, n_wrong)

        dups = j.filter(F.col("expected_class") == "DUPLICATE")
        misrouted = dups.filter(F.col("match_state") != STATE_DUPLICATE_SUSPECT)
        n_dups, n_mis = dups.count(), misrouted.count()
        r.add("C4", f"duplicate routing ({n_dups} seeded copies)",
              n_mis == 0 and n_dups > 0, n_mis)

        bank_exact = j.filter((F.col("leg") == "BANK") &
                              (F.col("match_state") == STATE_EXACT))
        n_be = bank_exact.count()
        r.add("C5", "zero exact matches on the bank leg", n_be == 0, n_be)

        print("\nexpected_class x match_state:")
        (j.groupBy("leg", "expected_class")
           .pivot("match_state")
           .count().na.fill(0)
           .orderBy("leg", "expected_class")
           .show(40, truncate=False))
    finally:
        r.render()
        spark.stop()

    if r.failed:
        print("PRECISION/RECALL HARNESS: FAILED")
        return 1
    print("PRECISION/RECALL HARNESS: ALL CHECKS PASS — Stage 0 signed off")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
