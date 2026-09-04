#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(REPO_ROOT, "scripts", "seed_generator.py")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default="2026-07-07")
    p.add_argument("--rows", type=int, default=30000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--whale", default="M007")
    p.add_argument("--whale-share", type=float, default=0.40)
    p.add_argument("--out", default=os.path.join(REPO_ROOT, "data", "fixtures"))
    a = p.parse_args(argv)

    if a.rows > 40000:
        print(f"WARNING: {a.rows} rows with a {a.whale_share:.0%} whale will put "
              f"~{int(a.rows * a.whale_share / 4)} records in one block. The "
              f"bank-leg candidate join is within-block; expect an OOM rather "
              f"than a straggler. 30000 is the tested size.", file=sys.stderr)

    cmd = [sys.executable, GEN, "--date", a.date, "--rows", str(a.rows),
           "--seed", str(a.seed), "--out", a.out,
           "--whale", a.whale, "--whale-share", str(a.whale_share)]
    rc = subprocess.run(cmd, cwd=REPO_ROOT).returncode
    if rc:
        return rc
    print(f"\nskew fixture ready for {a.date}: {a.whale} carries "
          f"~{a.whale_share:.0%} of {a.rows} rows")
    print("next: make_manifest -> upload_landing -> canonicalize_job -> recon_job")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
