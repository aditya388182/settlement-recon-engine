#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from pyspark.sql import DataFrame                                    
from pyspark.sql import functions as F                            

from spark.common.io import read_canonical                  
from spark.common.session import (DEFAULT_CONFIG, build_spark,      
                                  load_config)
from spark.recon import blocking, exact_match, resolver, tolerant_match  
from spark.recon.control_totals import (ControlTotalViolation,    
                                        assert_control_totals)
from spark.recon.output import write_recon_output                 
from spark.recon.tolerance import derive_tolerances                  # noqa: E402 all the improts above

LEG_PROCESSOR = "PROCESSOR"
LEG_BANK = "BANK"

STATE_EXACT = "EXACT_MATCHED"
STATE_TOLERANT = "TOLERANT_MATCHED"
STATE_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
STATE_UNMATCHED = "UNMATCHED"

EVIDENCE_COLS = ["counterpart_txn_uid", "counterpart_ref", "amount_diff",
                 "fee_residual", "date_diff", "score", "method", "tier",
                 "hungarian_cost", "candidate_count", "tolerance_applied",
                 "block_key"]
_NULL_TYPES = {"counterpart_txn_uid": "string", "counterpart_ref": "string",
               "amount_diff": "bigint", "fee_residual": "bigint",
               "date_diff": "int", "score": "double",
               "method": "string", "tier": "string", "hungarian_cost": "double",
               "candidate_count": "int", "tolerance_applied": "bigint",
               "block_key": "string"}


def _ledger(df: DataFrame, leg: str) -> DataFrame:
    return df.select("source_system", F.lit(leg).alias("leg"), "txn_uid",
                     "currency", "amount_minor")


def _base(df: DataFrame, leg: str, state: str, business_date: str) -> DataFrame:
    return (df.select("txn_uid", "txn_ref", "amount_minor", "currency",
                      "source_system",
                      F.col("business_date").alias("row_business_date"))
            .withColumn("business_date", F.lit(business_date).cast("date"))
            .withColumn("leg", F.lit(leg))
            .withColumn("match_state", F.lit(state)))


def _with_evidence(df: DataFrame, leg: str, state: str, business_date: str,
                   assignments: DataFrame | None, side: str) -> DataFrame:
    base = _base(df, leg, state, business_date)
    if assignments is None:
        for c in EVIDENCE_COLS:
            base = base.withColumn(c, F.lit(None).cast(_NULL_TYPES[c]))
        return base
    me, other = ("l", "r") if side == "left" else ("r", "l")
    ev = assignments.select(
        F.col(f"{me}_txn_uid").alias("txn_uid"),
        F.col(f"{other}_txn_uid").alias("counterpart_txn_uid"),
        F.col(f"{other}_txn_ref").alias("counterpart_ref"),
        F.col("amount_diff").cast("bigint").alias("amount_diff"),
        F.col("fee_residual").cast("bigint").alias("fee_residual"),
        F.col("date_diff").cast("int").alias("date_diff"),
        F.col("score").cast("double").alias("score"),
        F.col("method"), F.col("tier"),
        F.col("hungarian_cost").cast("double").alias("hungarian_cost"),
        F.col("candidate_count").cast("int").alias("candidate_count"),
        F.col("tolerance_applied").cast("bigint").alias("tolerance_applied"),
        F.col("block_key"))
    return base.join(ev, on="txn_uid", how="inner")


def _exact_as_assignments(pairs: DataFrame) -> DataFrame:
    """The exact pass produces the same evidence shape as the resolver so the
    output has one schema, not two."""
    return (pairs
            .withColumn("amount_diff", F.lit(0).cast("bigint"))
            .withColumn("fee_residual", F.lit(0).cast("bigint"))
            .withColumn("date_diff", F.lit(0).cast("int"))
            .withColumn("score", F.lit(1.0))
            .withColumn("method", F.lit("EXACT"))
            .withColumn("tier", F.lit("T1"))
            .withColumn("hungarian_cost", F.lit(None).cast("double"))
            .withColumn("candidate_count", F.lit(1).cast("int"))
            .withColumn("tolerance_applied", F.lit(0).cast("bigint")))


def run(spark, cfg: dict, business_date: str, chaos_drop_one: bool = False,
        force_greedy: bool = False) -> int:
    """cfg["paths"]["recon_output"] may have been overridden by --output-path so
    the force-greedy comparison run can be written somewhere the harness can
    diff it against the real one."""
    window_days = int(cfg["matching"]["settlement_window_days"])

    canon = {s: read_canonical(spark, cfg, s, business_date).cache()
             for s in ("internal", "processor", "bank")}
    for s, df in canon.items():
        print(f"[read] canonical/{s}: {df.count()} rows in window")

    input_ledger = (
        _ledger(canon["internal"], LEG_PROCESSOR)
        .unionByName(_ledger(canon["internal"], LEG_BANK))
        .unionByName(_ledger(canon["processor"], LEG_PROCESSOR))
        .unionByName(_ledger(canon["bank"], LEG_BANK))
    ).cache()

    clean, suspects = {}, {}
    for s in ("internal", "processor", "bank"):
        c, d = exact_match.route_duplicate_suspects(canon[s])
        clean[s], suspects[s] = c.cache(), d.cache()
        n = suspects[s].count()
        if n:
            print(f"[dedupe] {s}: {n} duplicate suspect(s) routed out of matching")

    blocked = {s: blocking.with_block_keys(clean[s], cfg).cache()
               for s in ("internal", "processor", "bank")}
    for s in ("internal", "processor", "bank"):
        blocking.block_stats(blocked[s], cfg, s)

    internal_refs = clean["internal"].select("txn_ref").distinct().cache()
    tolerances = derive_tolerances(spark, cfg, business_date).cache()
    print(f"[tolerance] derived point-in-time as of business_date={business_date}:")
    tolerances.orderBy("currency").show(truncate=False)

    frames = []
    for leg, right_src, fee_adjust in ((LEG_PROCESSOR, "processor", False),
                                       (LEG_BANK, "bank", True)):
        pairs, l_res, r_res = exact_match.exact_match(blocked["internal"],
                                                      blocked[right_src])
        pairs = pairs.cache()
        exact_match.assert_one_to_one(pairs, f"leg {leg} exact")
        print(f"[pass1 exact:{leg}] {pairs.count()} pair(s)")

        bl = blocked["internal"].join(l_res, "txn_uid", "left_semi")
        br = blocked[right_src].join(r_res, "txn_uid", "left_semi")

        c2 = tolerant_match.generate_candidates(
            bl, br, tolerances, window_days,
            ref_anchored=True, fee_adjust=fee_adjust).cache()
        a2 = resolver.resolve(c2, cfg, force_greedy).cache()
        print(f"[pass2 tolerant-T2:{leg}] {c2.count()} candidate(s) -> "
              f"{a2.count()} assignment(s)")

        bl2 = bl.join(a2.select(F.col("l_txn_uid").alias("txn_uid")).distinct(),
                      "txn_uid", "left_anti")
        br2 = br.join(a2.select(F.col("r_txn_uid").alias("txn_uid")).distinct(),
                      "txn_uid", "left_anti")
        # Tier 3 is for counterparty rows whose reference cannot be resolved
        # against ANY internal reference — a settlement/batch ref rather than a
        # transaction id. A row that still carries a resolvable ref and did not
        # match in tier 2 has a real problem (amount beyond tolerance, or a
        # missing counterpart); pairing it to a different transaction that
        # happens to sit within tolerance would be a fabricated match, not a
        # recovered one. On the processor leg every ref resolves, so this
        # filter empties tier 3 entirely, which is correct.
        br2 = br2.join(F.broadcast(internal_refs), "txn_ref", "left_anti")

        c3 = tolerant_match.generate_candidates(
            bl2, br2, tolerances, window_days,
            ref_anchored=False, fee_adjust=fee_adjust).cache()
        dens = tolerant_match.candidate_density(c3).cache()
        threshold = float(cfg["matching"]["ambiguity_density_threshold"])
        n_over = dens.filter(F.col("density") > threshold).count()
        row = dens.selectExpr("count(*) as n", "max(density) as mx",
                              "percentile_approx(density, 0.99) as p99").collect()[0]
        print(f"[pass3 density:{leg}] blocks={row['n']} max={row['mx']} "
              f"p99={row['p99']} above_threshold={n_over}")
        a3 = resolver.resolve(c3, cfg, force_greedy).cache()
        print(f"[pass3 tolerant-T3:{leg}] {c3.count()} candidate(s) -> "
              f"{a3.count()} assignment(s)")

        assignments = a2.unionByName(a3).cache()
        resolver.assert_one_to_one(assignments, f"leg {leg} tolerant")

        matched_l = pairs.select(F.col("l_txn_uid").alias("txn_uid")).distinct()
        matched_r = pairs.select(F.col("r_txn_uid").alias("txn_uid")).distinct()
        tol_l = assignments.select(F.col("l_txn_uid").alias("txn_uid")).distinct()
        tol_r = assignments.select(F.col("r_txn_uid").alias("txn_uid")).distinct()
        li, ri = clean["internal"], clean[right_src]
        ex_assign = _exact_as_assignments(pairs)

        frames += [
            _with_evidence(li.join(matched_l, "txn_uid", "left_semi"),
                           leg, STATE_EXACT, business_date, ex_assign, "left"),
            _with_evidence(ri.join(matched_r, "txn_uid", "left_semi"),
                           leg, STATE_EXACT, business_date, ex_assign, "right"),
            _with_evidence(li.join(tol_l, "txn_uid", "left_semi"),
                           leg, STATE_TOLERANT, business_date, assignments, "left"),
            _with_evidence(ri.join(tol_r, "txn_uid", "left_semi"),
                           leg, STATE_TOLERANT, business_date, assignments, "right"),
            _with_evidence(li.join(matched_l, "txn_uid", "left_anti")
                             .join(tol_l, "txn_uid", "left_anti"),
                           leg, STATE_UNMATCHED, business_date, None, "left"),
            _with_evidence(ri.join(matched_r, "txn_uid", "left_anti")
                             .join(tol_r, "txn_uid", "left_anti"),
                           leg, STATE_UNMATCHED, business_date, None, "right"),
        ]

    for src, leg in (("internal", LEG_PROCESSOR), ("internal", LEG_BANK),
                     ("processor", LEG_PROCESSOR), ("bank", LEG_BANK)):
        frames.append(_with_evidence(suspects[src], leg, STATE_DUPLICATE_SUSPECT,
                                     business_date, None, "left"))

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    output = frames[0]
    for f in frames[1:]:
        output = output.unionByName(f)
    output = output.withColumn("run_ts", F.lit(run_ts)).cache()
    print(f"[union] {output.count()} output rows at "
          f"(source_system, txn_uid, leg) grain")
    (output.groupBy("leg", "match_state").count()
     .orderBy("leg", "match_state").show(20, truncate=False))
    (output.filter(F.col("method").isNotNull())
     .groupBy("leg", "tier", "method").count()
     .orderBy("leg", "tier", "method").show(20, truncate=False))

    if chaos_drop_one:
        # The drop must hit the OUTPUT, after the union. Dropping a PAIR only
        # demotes the record to a residual and conservation still holds, so the
        # run stays green and the sabotage proves nothing.
        victim = (output.filter((F.col("match_state") == STATE_EXACT) &
                                (F.col("source_system") == "INTERNAL"))
                  .orderBy("txn_uid").limit(1).collect()[0])
        print(f"[CHAOS] dropping {victim['source_system']}/{victim['leg']}/"
              f"{victim['txn_uid']} ({victim['currency']} "
              f"{victim['amount_minor']} minor) after resolution, before union")
        output = output.filter(
            ~((F.col("txn_uid") == victim["txn_uid"]) &
              (F.col("leg") == victim["leg"]) &
              (F.col("source_system") == victim["source_system"]))).cache()

    assert_control_totals(input_ledger, output, business_date)
    write_recon_output(output, cfg, business_date)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--chaos-drop-one", action="store_true",
                   help="silently drop one matched record; control totals must "
                        "fail closed (Day 4 sabotage test)")
    p.add_argument("--output-path", default=None,
                   help="override paths.recon_output (used for the force-greedy "
                        "comparison run)")
    p.add_argument("--force-greedy", action="store_true",
                   help="disable the Hungarian fallback (threshold -> infinity). "
                        "The harness uses this to prove greedy differs on the "
                        "seeded dense blocks.")
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    if a.output_path:
        cfg["paths"]["recon_output"] = a.output_path
    spark = build_spark(f"recon-{a.date}", cfg)
    try:
        return run(spark, cfg, a.date, a.chaos_drop_one, a.force_greedy)
    except ControlTotalViolation as e:
        print(f"\nControlTotalViolation: {e}", file=sys.stderr)
        print("NOTHING WAS WRITTEN. The previous partition is untouched.",
              file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())