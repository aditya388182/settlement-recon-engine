#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F                                
from spark.common.io import read_recon_output, storage_format       
from spark.common.session import (DEFAULT_CONFIG, build_spark,        # noqa: E402
                                  load_config)

JOIN_KEYS = ["source_system", "txn_uid", "leg"]
STATE_EXACT = "EXACT_MATCHED"
STATE_TOLERANT = "TOLERANT_MATCHED"
STATE_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
MATCHED_STATES = (STATE_EXACT, STATE_TOLERANT)
SHOULD_MATCH = ("MATCHED", "TIMING_DIFFERENCE")
SHOULD_NOT_MATCH = ("AMOUNT_MISMATCH", "MISSING_IN_PROCESSOR",
                    "MISSING_IN_BANK", "DUPLICATE")


class Result:
    def __init__(self):
        self.rows = []
        self.failed = False

    def add(self, check: str, detail: str, ok: bool, n: int = 0):
        self.rows.append((check, detail, "PASS" if ok else "FAIL", n))
        if not ok:
            self.failed = True

    def render(self) -> None:
        if not self.rows:
            return
        w = max(len(r[1]) for r in self.rows) + 2
        print()
        print(f"{'CHECK':<7}{'ASSERTION':<{w}}{'RESULT':<8}{'OFFENDING':>10}")
        print("-" * (7 + w + 18))
        for check, detail, res, n in self.rows:
            print(f"{check:<7}{detail:<{w}}{res:<8}{n:>10}")
        print("-" * (7 + w + 18))


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--fixtures", default="data/fixtures")
    p.add_argument("--greedy-output-path", default=None,
                   help="output of a --force-greedy run; enables C10")
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    spark = build_spark(f"precision-recall-{a.date}", cfg)
    r = Result()
    try:
        out = read_recon_output(spark, cfg, a.date).cache()
        key = (spark.read.option("header", "true")
               .csv(os.path.join(a.fixtures, f"answer_key_{a.date}.csv")).cache())

        n_ok = key.join(out, JOIN_KEYS, "left_anti").count()
        n_oo = out.join(key, JOIN_KEYS, "left_anti").count()
        r.add("C1", "answer key rows without an output row", n_ok == 0, n_ok)
        r.add("C1", "output rows without an answer key row", n_oo == 0, n_oo)

        j = key.join(out, JOIN_KEYS, "inner").cache()
        matched = F.col("match_state").isin(list(MATCHED_STATES))

        want = j.filter((F.col("leg") == "PROCESSOR") &
                        (F.col("expected_class") == "MATCHED"))
        missed = want.filter(F.col("match_state") != STATE_EXACT)
        n_want, n_missed = want.count(), missed.count()
        rec = 0.0 if not n_want else (n_want - n_missed) / n_want
        r.add("C2", f"exact recall, leg PROCESSOR ({rec:.4%} of {n_want})",
              n_missed == 0, n_missed)

        wrong = j.filter(matched & F.col("expected_class").isin(list(SHOULD_NOT_MATCH)))
        n_wrong = wrong.count()
        if n_wrong:
            wrong.groupBy("leg", "expected_class", "match_state").count() \
                 .show(20, truncate=False)
        r.add("C3", "matched rows the key calls a break", n_wrong == 0, n_wrong)

        dups = j.filter(F.col("expected_class") == "DUPLICATE")
        mis = dups.filter(F.col("match_state") != STATE_DUPLICATE_SUSPECT).count()
        r.add("C4", f"duplicate routing ({dups.count()} seeded copies)",
              mis == 0, mis)

        n_be = j.filter((F.col("leg") == "BANK") &
                        (F.col("match_state") == STATE_EXACT)).count()
        r.add("C5", "zero exact matches on the bank leg", n_be == 0, n_be)

        for klass in SHOULD_MATCH:
            sub = j.filter(F.col("expected_class") == klass)
            n_sub = sub.count()
            n_miss = sub.filter(~matched).count()
            rc = 0.0 if not n_sub else (n_sub - n_miss) / n_sub
            r.add("C6", f"recall for {klass} ({rc:.4%} of {n_sub})",
                  n_miss == 0, n_miss)

        tim = j.filter((F.col("expected_class") == "TIMING_DIFFERENCE") & matched)
        bad_t = tim.filter(F.coalesce(F.col("date_diff"), F.lit(0)) == 0).count()
        r.add("C7", f"TIMING_DIFFERENCE rows carry date_diff > 0 ({tim.count()})",
              bad_t == 0, bad_t)

        traps = j.filter((F.col("trap_group_id") != "") & (F.col("leg") == "BANK"))
        group_bank_refs = (traps.filter(F.col("source_system") == "BANK")
                           .select("trap_group_id", F.col("txn_ref").alias("bank_ref")))
        assigned = (traps.filter(F.col("source_system") == "INTERNAL")
                    .select("trap_group_id", "txn_uid", "match_state",
                            F.col("counterpart_ref").alias("bank_ref")))
        unmatched_traps = assigned.filter(~matched).count()
        outside = assigned.join(group_bank_refs, ["trap_group_id", "bank_ref"],
                                "left_anti").count()
        per_group = (assigned.groupBy("trap_group_id")
                     .agg(F.countDistinct("bank_ref").alias("d"),
                          F.count("*").alias("n")))
        not_1to1 = per_group.filter("d <> n OR n <> 2").count()
        n_groups = per_group.count()
        r.add("C8", f"trap groups resolve one-to-one in-group ({n_groups} groups)",
              unmatched_traps == 0 and outside == 0 and not_1to1 == 0,
              unmatched_traps + outside + not_1to1)

        hung = j.filter(F.col("expects_hungarian") == "true")
        n_h = hung.count()
        not_h = hung.filter(F.coalesce(F.col("method"), F.lit("")) != "HUNGARIAN").count()
        r.add("C9", f"expects_hungarian rows decided by HUNGARIAN ({n_h})",
              n_h > 0 and not_h == 0, not_h)

        if a.greedy_output_path:
            g = (spark.read.format(storage_format(cfg)).load(a.greedy_output_path)
                 .filter(F.col("business_date") == F.lit(a.date).cast("date"))
                 .select(*JOIN_KEYS,
                         F.col("counterpart_ref").alias("greedy_counterpart"),
                         F.col("match_state").alias("greedy_state")))
            gd = (j.filter(F.col("greedy_differs") == "true")
                  .join(g, JOIN_KEYS, "inner"))
            same = gd.filter(
                F.coalesce(F.col("counterpart_ref"), F.lit("~")) ==
                F.coalesce(F.col("greedy_counterpart"), F.lit("~"))).count()
            n_gd = gd.count()
            r.add("C10", f"greedy differs on greedy_differs rows ({n_gd})",
                  n_gd > 0 and same == 0, same)
        else:
            print("C10 skipped — pass --greedy-output-path to enable it")

        print("\nexpected_class x match_state:")
        (j.groupBy("leg", "expected_class").pivot("match_state").count()
           .na.fill(0).orderBy("leg", "expected_class").show(40, truncate=False))
        print("tier x method (matched rows only):")
        (j.filter(matched).groupBy("leg", "tier", "method").count()
           .orderBy("leg", "tier", "method").show(20, truncate=False))
    finally:
        r.render()
        spark.stop()

    if r.failed:
        print("PRECISION/RECALL HARNESS: FAILED")
        return 1
    print("PRECISION/RECALL HARNESS: ALL CHECKS PASS — Stage 1 signed off")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())