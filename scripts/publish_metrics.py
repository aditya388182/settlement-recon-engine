#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F                                # noqa: E402
from spark.common.io import read_recon_output, storage_format         # noqa: E402
from spark.common.session import (DEFAULT_CONFIG, build_spark,        # noqa: E402
                                  load_config)

PUSHGATEWAY = "http://localhost:9091/metrics/job/recon_daily"
BREAK_CLASSES = ("AMOUNT_MISMATCH", "MISSING_IN_PROCESSOR", "MISSING_IN_BANK",
                 "DUPLICATE", "TIMING_DIFFERENCE")


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_exposition(spark, cfg: dict, business_date: str,
                     duration_seconds: float) -> str:
    lines: list[str] = []
    ok = 1
    rows_published = 0
    try:
        out = read_recon_output(spark, cfg, business_date).cache()
        rows_published = out.count()
        if rows_published == 0:
            ok = 0
    except Exception as exc:                       # noqa: BLE001
        print(f"[metrics] no readable output partition for {business_date}: {exc}")
        ok = 0
        out = None

    if out is not None and rows_published:
        summary = (out.groupBy("leg", "break_class", "currency")
                   .agg(F.count("*").alias("n"),
                        F.sum("amount_minor").alias("amt"))
                   .collect())
        break_total = 0
        for r in summary:
            lines.append(
                f'recon_break_count{{class="{_escape(r["break_class"])}",'
                f'currency="{_escape(r["currency"])}",'
                f'leg="{_escape(r["leg"])}"}} {r["n"]}')
            if r["break_class"] in BREAK_CLASSES:
                break_total += int(r["amt"] or 0)
        lines.append(f"recon_break_amount_minor_total {break_total}")

        n_hung = (out.filter(F.col("evidence.method") == "HUNGARIAN")
                  .select("block_key").distinct().count())
        lines.append(f"recon_hungarian_activations {n_hung}")

    lines.append(f"recon_rows_published {rows_published}")
    lines.append(f"recon_run_duration_seconds {duration_seconds:.1f}")
    lines.append(f"recon_control_totals_ok {ok}")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--started-at", default=None,
                   help='ISO timestamp the run began; the DAG passes '
                        '"{{ dag_run.start_date }}"')
    p.add_argument("--pushgateway", default=PUSHGATEWAY)
    p.add_argument("--dry-run", action="store_true",
                   help="print the exposition text instead of pushing it")
    a = p.parse_args(argv)

    duration = 0.0
    if a.started_at:
        try:
            started = datetime.fromisoformat(a.started_at.replace("Z", "+00:00"))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            duration = (datetime.now(timezone.utc) - started).total_seconds()
        except ValueError:
            print(f"[metrics] unparseable --started-at {a.started_at!r}; "
                  f"duration will be 0")

    cfg = load_config(a.config)
    spark = build_spark(f"publish-metrics-{a.date}", cfg)
    try:
        body = build_exposition(spark, cfg, a.date, duration)
    finally:
        spark.stop()

    print(body, end="")
    if a.dry_run:
        return 0

    import requests
    resp = requests.post(a.pushgateway, data=body.encode("utf-8"), timeout=10)
    if resp.status_code >= 300:
        print(f"[metrics] pushgateway returned {resp.status_code}: {resp.text}",
              file=sys.stderr)
        return 1
    print(f"[metrics] pushed {len(body.splitlines())} series to {a.pushgateway}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
