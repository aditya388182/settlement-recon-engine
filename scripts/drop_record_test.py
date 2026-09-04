#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.byte_identity_check import output_hash                  # noqa: E402
from spark.common.session import (DEFAULT_CONFIG, build_spark,       # noqa: E402
                                  load_config)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB = os.environ.get("RECON_JOB_ENTRYPOINT",
                     os.path.join(REPO_ROOT, "spark", "jobs", "recon_job.py"))


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    a = p.parse_args(argv)
    cfg = load_config(a.config)

    spark = build_spark("drop-record-before", cfg)
    try:
        before, n_before = output_hash(spark, cfg, a.date)
    finally:
        spark.stop()
    print(f"partition before sabotage: sha256={before} rows={n_before}")

    proc = subprocess.run(
        [sys.executable, JOB, "--date", a.date, "--config", a.config,
         "--chaos-drop-one"],
        cwd=REPO_ROOT, capture_output=True, text=True)
    print(proc.stdout[-2000:])
    print(proc.stderr[-2000:], file=sys.stderr)

    failures = []
    if proc.returncode == 0:
        failures.append("the sabotaged run exited 0 — the drop did not reach "
                        "the output, or control totals did not fire")
    if "ControlTotalViolation" not in proc.stderr:
        failures.append("no ControlTotalViolation in stderr")

    spark = build_spark("drop-record-after", cfg)
    try:
        after, n_after = output_hash(spark, cfg, a.date)
    finally:
        spark.stop()
    print(f"partition after sabotage:  sha256={after} rows={n_after}")
    if after != before:
        failures.append(f"the partition CHANGED: {before} -> {after}")

    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1
    print(f"\nFAIL-CLOSED PASS — exit 1, ControlTotalViolation raised, and the "
          f"{n_before}-row partition for {a.date} is unchanged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
