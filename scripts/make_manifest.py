#!/usr/bin/env python3
"""make_manifest.py — row counts + SHA256 per source file.

This manifest is the input to the Great Expectations checksum gate (Day 2/5).
It hashes the EXACT BYTES ON DISK, not a parsed representation: a file that is
truncated in transit, re-encoded, or has its line endings rewritten must fail
the gate, and only a byte hash catches all three.

    python scripts/make_manifest.py --date 2026-07-06 \
        --fixtures data/fixtures --out data/manifests
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os

SOURCES = ("internal", "processor", "bank")


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def count_data_rows(path: str) -> int:
    """Row count excluding the header. Counted on bytes, not via csv, so a
    ragged file still produces a number the band check can reject."""
    with open(path, "rb") as fh:
        n = sum(1 for _ in fh)
    return max(n - 1, 0)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--fixtures", default="data/fixtures")
    p.add_argument("--out", default="data/manifests")
    a = p.parse_args(argv)

    files = {}
    for src in SOURCES:
        name = f"{src}_{a.date}.csv"
        path = os.path.join(a.fixtures, name)
        if not os.path.exists(path):
            raise SystemExit(f"missing source file: {path}")
        files[name] = {"rows": count_data_rows(path), "sha256": sha256_of(path)}

    os.makedirs(a.out, exist_ok=True)
    manifest = {"business_date": a.date, "files": files}
    out_path = os.path.join(a.out, f"manifest_{a.date}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")

    print(f"wrote {out_path}")
    for name, meta in sorted(files.items()):
        print(f"  {name:<28} {meta['rows']:>7} rows  {meta['sha256'][:16]}...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
