#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

import boto3
import yaml
from botocore.client import Config

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = os.path.join(REPO_ROOT, "conf", "recon_config.yml")
SOURCES = ("internal", "processor", "bank")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--dest", required=True)
    p.add_argument("--bucket", default="recon-landing")
    p.add_argument("--config", default=CONFIG)
    a = p.parse_args(argv)

    with open(a.config, encoding="utf-8") as fh:
        s = yaml.safe_load(fh)["spark"]
    s3 = boto3.client("s3", endpoint_url=s["s3_endpoint"],
                      aws_access_key_id=s["s3_access_key"],
                      aws_secret_access_key=s["s3_secret_key"],
                      config=Config(signature_version="s3v4"),
                      region_name="us-east-1")

    os.makedirs(a.dest, exist_ok=True)
    keys = [f"{a.date}/{src}_{a.date}.csv" for src in SOURCES]
    keys.append(f"{a.date}/manifest_{a.date}.json")
    for key in keys:
        local = os.path.join(a.dest, os.path.basename(key))
        s3.download_file(a.bucket, key, local)
        print(f"staged {key} -> {local} ({os.path.getsize(local):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
