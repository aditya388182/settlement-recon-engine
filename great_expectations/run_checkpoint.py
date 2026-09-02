#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, timedelta

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUITE_DIR = os.path.join(REPO_ROOT, "great_expectations", "expectations")
SOURCES = ("internal", "processor", "bank")
ROW_BAND = 0.10


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def date_window(business_date: str, window_days: int) -> list[str]:
    d = date.fromisoformat(business_date)
    return [(d + timedelta(days=i)).isoformat() for i in range(window_days + 1)]


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, choices=SOURCES)
    p.add_argument("--date", required=True)
    p.add_argument("--fixtures", default=os.path.join(REPO_ROOT, "data", "fixtures"))
    p.add_argument("--manifests", default=os.path.join(REPO_ROOT, "data", "manifests"))
    p.add_argument("--window-days", type=int, default=2)
    a = p.parse_args(argv)

    name = f"{a.source}_{a.date}.csv"
    data_path = os.path.join(a.fixtures, name)
    manifest_path = os.path.join(a.manifests, f"manifest_{a.date}.json")
    failures: list[str] = []
    checks = 0

    if not os.path.exists(data_path):
        print(f"SUITE {a.source}_{a.date}: FAIL — missing file {data_path}")
        return 1
    if not os.path.exists(manifest_path):
        print(f"SUITE {a.source}_{a.date}: FAIL — missing manifest {manifest_path}")
        return 1

    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    entry = manifest.get("files", {}).get(name)
    if entry is None:
        print(f"SUITE {a.source}_{a.date}: FAIL — {name} absent from the manifest")
        return 1

    checks += 1
    actual_sha = sha256_of(data_path)
    if actual_sha != entry["sha256"]:
        failures.append(f"sha256 mismatch: file {actual_sha[:16]}... "
                        f"manifest {entry['sha256'][:16]}...")

    df = pd.read_csv(data_path, dtype=str, keep_default_na=False)
    checks += 1
    lo = int(entry["rows"] * (1 - ROW_BAND))
    hi = int(entry["rows"] * (1 + ROW_BAND))
    if not lo <= len(df) <= hi:
        failures.append(f"row count {len(df)} outside band [{lo}, {hi}] "
                        f"(manifest says {entry['rows']})")

    import great_expectations as gx
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.expectation_configuration import ExpectationConfiguration

    with open(os.path.join(SUITE_DIR, f"{a.source}_suite.json"), encoding="utf-8") as fh:
        suite_json = json.load(fh)
    expectations = [ExpectationConfiguration(**e) for e in suite_json["expectations"]]
    expectations.append(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "business_date",
                "value_set": date_window(a.date, a.window_days)}))
    suite = ExpectationSuite(expectation_suite_name=suite_json["expectation_suite_name"],
                             expectations=expectations)

    context = gx.get_context(mode="ephemeral")
    context.add_or_update_expectation_suite(expectation_suite=suite)
    asset = (context.sources.add_or_update_pandas(f"gate_{a.source}")
             .add_dataframe_asset(name=f"{a.source}_{a.date}"))
    validator = context.get_validator(
        batch_request=asset.build_batch_request(dataframe=df),
        expectation_suite_name=suite.expectation_suite_name)
    result = validator.validate(result_format="BASIC")

    checks += len(result.results)
    for r in result.results:
        if not r.success:
            cfg = r.expectation_config
            col = cfg.kwargs.get("column", "<table>")
            failures.append(f"{cfg.expectation_type} on {col}: "
                            f"{r.result.get('unexpected_count', 'failed')} unexpected")

    passed = checks - len(failures)
    if failures:
        print(f"SUITE {a.source}_{a.date}: FAIL ({passed}/{checks} checks)")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"SUITE {a.source}_{a.date}: PASS ({passed}/{checks} checks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
