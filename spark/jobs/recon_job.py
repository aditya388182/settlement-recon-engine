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
from spark.common.session import build_spark, load_config, DEFAULT_CONFIG  
from spark.recon import blocking, exact_match                        
from spark.recon.control_totals import (ControlTotalViolation,      
                                        assert_control_totals)
from spark.recon.exact_match import (STATE_DUPLICATE_SUSPECT,        
                                     STATE_EXACT, STATE_PENDING)
from spark.recon.output import write_recon_output                    # noqa: E402 for the improts

LEG_PROCESSOR = "PROCESSOR"
LEG_BANK = "BANK"


def _ledger(df: DataFrame, leg: str) -> DataFrame:
    """Project a canonical source onto the control-total ledger grain."""
    return df.select("source_system", F.lit(leg).alias("leg"), "txn_uid",
                     "currency", "amount_minor")


def _state_rows(df: DataFrame, leg: str, state: str, business_date: str,
                pairs: DataFrame | None, side: str) -> DataFrame:
    """Attach match state and counterpart columns to one side of one leg."""
    base = df.select("txn_uid", "txn_ref", "amount_minor", "currency",
                     "source_system",
                     F.col("business_date").alias("row_business_date"))
    if pairs is None:
        joined = (base
                  .withColumn("counterpart_txn_uid", F.lit(None).cast("string"))
                  .withColumn("counterpart_ref", F.lit(None).cast("string"))
                  .withColumn("block_key", F.lit(None).cast("string")))
    else:
        me, other = ("l", "r") if side == "left" else ("r", "l")
        p = pairs.select(F.col(f"{me}_txn_uid").alias("txn_uid"),
                         F.col(f"{other}_txn_uid").alias("counterpart_txn_uid"),
                         F.col(f"{other}_txn_ref").alias("counterpart_ref"),
                         "block_key")
        joined = base.join(p, on="txn_uid", how="inner")
    return (joined
            .withColumn("business_date", F.lit(business_date).cast("date"))
            .withColumn("leg", F.lit(leg))
            .withColumn("match_state", F.lit(state)))


def run(spark, cfg: dict, business_date: str, chaos_drop_one: bool = False) -> int:
    canon = {s: read_canonical(spark, cfg, s, business_date).cache()
             for s in ("internal", "processor", "bank")}
    for s, df in canon.items():
        print(f"[read] canonical/{s}: {df.count()} rows in window")

    # The control-total ledger is fixed the moment the run has read its input.
    # Internal appears TWICE because it participates in both legs.
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

    frames = []
    for leg, right_src in ((LEG_PROCESSOR, "processor"), (LEG_BANK, "bank")):
        pairs, l_res, r_res = exact_match.exact_match(blocked["internal"],
                                                      blocked[right_src])
        pairs = pairs.cache()
        exact_match.assert_one_to_one(pairs, f"leg {leg}")
        n_pairs = pairs.count()
        print(f"[exact:{leg}] {n_pairs} pair(s) matched")

        matched_l = pairs.select(F.col("l_txn_uid").alias("txn_uid")).distinct()
        matched_r = pairs.select(F.col("r_txn_uid").alias("txn_uid")).distinct()
        left_clean = clean["internal"]
        right_clean = clean[right_src]

        frames.append(_state_rows(left_clean.join(matched_l, "txn_uid", "left_semi"),
                                  leg, STATE_EXACT, business_date, pairs, "left"))
        frames.append(_state_rows(right_clean.join(matched_r, "txn_uid", "left_semi"),
                                  leg, STATE_EXACT, business_date, pairs, "right"))
        frames.append(_state_rows(left_clean.join(matched_l, "txn_uid", "left_anti"),
                                  leg, STATE_PENDING, business_date, None, "left"))
        frames.append(_state_rows(right_clean.join(matched_r, "txn_uid", "left_anti"),
                                  leg, STATE_PENDING, business_date, None, "right"))

    # duplicate suspects never entered matching; they still owe the ledger a row
    frames.append(_state_rows(suspects["internal"], LEG_PROCESSOR,
                              STATE_DUPLICATE_SUSPECT, business_date, None, "left"))
    frames.append(_state_rows(suspects["internal"], LEG_BANK,
                              STATE_DUPLICATE_SUSPECT, business_date, None, "left"))
    frames.append(_state_rows(suspects["processor"], LEG_PROCESSOR,
                              STATE_DUPLICATE_SUSPECT, business_date, None, "left"))
    frames.append(_state_rows(suspects["bank"], LEG_BANK,
                              STATE_DUPLICATE_SUSPECT, business_date, None, "left"))

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    output = frames[0]
    for f in frames[1:]:
        output = output.unionByName(f)
    output = output.withColumn("run_ts", F.lit(run_ts)).cache()
    print(f"[union] {output.count()} output rows at "
          f"(source_system, txn_uid, leg) grain")

    if chaos_drop_one:
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
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    spark = build_spark(f"recon-{a.date}", cfg)
    try:
        return run(spark, cfg, a.date, a.chaos_drop_one)
    except ControlTotalViolation as e:
        print(f"\nControlTotalViolation: {e}", file=sys.stderr)
        print("NOTHING WAS WRITTEN. The previous partition is untouched.",
              file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
