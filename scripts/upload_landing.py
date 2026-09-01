#!/usr/bin/env python3
"""upload_landing.py — put the day's source files into the MinIO landing zone.

This is the prefix the Day-5 S3KeySensors watch, so the key layout is a
contract, not a convenience:

    recon-landing/<business_date>/internal_<business_date>.csv
    recon-landing/<business_date>/processor_<business_date>.csv
    recon-landing/<business_date>/bank_<business_date>.csv
    recon-landing/<business_date>/manifest_<business_date>.json

Uses boto3 against the MinIO endpoint from conf/recon_config.yml rather than the
`mc` container, so it does not depend on the compose network name and can be
run from the same venv as everything else.

    python scripts/upload_landing.py --date 2026-07-06
"""
from __future__ import annotations

import argparse
import os
import sys

import boto3
from botocore.client import Config

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import yaml  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = os.path.join(REPO_ROOT, "conf", "recon_config.yml")
SOURCES = ("internal", "processor", "bank")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--fixtures", default=os.path.join(REPO_ROOT, "data", "fixtures"))
    p.add_argument("--manifests", default=os.path.join(REPO_ROOT, "data", "manifests"))
    p.add_argument("--bucket", default="recon-landing")
    a = p.parse_args(argv)

    with open(CONFIG, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    s = cfg["spark"]

    s3 = boto3.client(
        "s3",
        endpoint_url=s["s3_endpoint"],
        aws_access_key_id=s["s3_access_key"],
        aws_secret_access_key=s["s3_secret_key"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    uploads = [(os.path.join(a.fixtures, f"{src}_{a.date}.csv"),
                f"{a.date}/{src}_{a.date}.csv") for src in SOURCES]
    uploads.append((os.path.join(a.manifests, f"manifest_{a.date}.json"),
                    f"{a.date}/manifest_{a.date}.json"))

    for local, key in uploads:
        if not os.path.exists(local):
            raise SystemExit(f"missing file: {local}")
        s3.upload_file(local, a.bucket, key)
        size = os.path.getsize(local)
        print(f"uploaded s3://{a.bucket}/{key}  ({size:,} bytes)")

    listing = s3.list_objects_v2(Bucket=a.bucket, Prefix=f"{a.date}/")
    keys = sorted(o["Key"] for o in listing.get("Contents", []))
    print(f"\nlanding zone now holds {len(keys)} object(s) under {a.date}/:")
    for k in keys:
        print(f"  {k}")
    if len(keys) != 4:
        print("EXPECTED 4 OBJECTS — the Day-5 sensors key off exactly these names")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
