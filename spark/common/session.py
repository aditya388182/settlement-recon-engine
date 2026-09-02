from __future__ import annotations

import os
import yaml
from pyspark.sql import SparkSession

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_CONFIG = os.path.join(REPO_ROOT, "conf", "recon_config.yml")

# Pinned for PySpark 3.5.1. delta-spark 3.1.0 targets Spark 3.5; hadoop-aws must
# match the Hadoop version Spark 3.5.1 is built against (3.3.4), and the AWS SDK
# must match hadoop-aws 3.3.4 (1.12.262). Do not "upgrade" any one of the three
# on its own.
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"


def load_config(path: str = DEFAULT_CONFIG) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _packages_and_conf(cfg: dict) -> tuple[str, dict]:
    """Return (spark.jars.packages, extra spark configs) for this cfg."""
    packages: list[str] = []
    conf: dict[str, str] = {}

    if cfg.get("storage", {}).get("format", "delta") == "delta":
        packages.append(DELTA_PACKAGE)
        conf["spark.sql.extensions"] = "io.delta.sql.DeltaSparkSessionExtension"
        conf["spark.sql.catalog.spark_catalog"] = (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

    if any(str(v).startswith("s3a://") for v in cfg.get("paths", {}).values()):
        packages += [HADOOP_AWS_PACKAGE, AWS_SDK_PACKAGE]
        s = cfg["spark"]
        conf.update({
            "spark.hadoop.fs.s3a.endpoint": s["s3_endpoint"],
            "spark.hadoop.fs.s3a.access.key": s["s3_access_key"],
            "spark.hadoop.fs.s3a.secret.key": s["s3_secret_key"],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        })

    return ",".join(packages), conf


def build_spark(app_name: str, cfg: dict | None = None) -> SparkSession:
    cfg = cfg or load_config()
    s = cfg["spark"]
    packages, extra = _packages_and_conf(cfg)
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # session-scoped UTC: the canonical schema stores UTC and nothing else
        .config("spark.sql.session.timeZone", "UTC")
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    for k, v in extra.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark