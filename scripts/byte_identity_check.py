#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark.common.io import read_recon_output                        # noqa: E402
from spark.common.session import (DEFAULT_CONFIG, build_spark,       # noqa: E402
                                  load_config)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB = os.environ.get("RECON_JOB_ENTRYPOINT",
                     os.path.join(REPO_ROOT, "spark", "jobs", "recon_job.py"))


def output_hash(spark, cfg: dict, business_date: str) -> tuple[str, int]:
    df = (read_recon_output(spark, cfg, business_date)
          .drop("run_ts")                       # the ONE column allowed to differ
          .orderBy("source_system", "leg", "txn_uid"))   # canonical order
    h = hashlib.sha256()
    n = 0
    for row in df.toLocalIterator():
        h.update(repr(tuple(row)).encode("utf-8"))
        n += 1
    return h.hexdigest(), n


def run_job(business_date: str, config: str) -> int:
    return subprocess.run([sys.executable, JOB, "--date", business_date,
                           "--config", config], cwd=REPO_ROOT).returncode


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--skip-runs", action="store_true",
                   help="hash the existing partition only (no engine runs)")
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    hashes = []
    for i in (1, 2):
        if not a.skip_runs:
            rc = run_job(a.date, a.config)
            if rc != 0:
                print(f"run {i} failed with exit code {rc}", file=sys.stderr)
                return rc
        spark = build_spark(f"byte-identity-{i}", cfg)
        try:
            digest, n = output_hash(spark, cfg, a.date)
        finally:
            spark.stop()
        print(f"run {i}: sha256={digest}  rows={n}")
        hashes.append(digest)
        if a.skip_runs:
            hashes.append(digest)
            break

    if hashes[0] == hashes[1]:
        print(f"\nBYTE-IDENTITY PASS — two runs of {a.date} produced identical "
              f"logical output\n  {hashes[0]}")
        return 0
    print(f"\nBYTE-IDENTITY FAIL — {hashes[0]} != {hashes[1]}", file=sys.stderr)
    print("diff the two sorted outputs; the first differing row names the "
          "nondeterminism", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
