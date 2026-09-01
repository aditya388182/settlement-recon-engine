"""Single Spark session builder for the whole project.

Every script imports this. Duplicating the s3a/Delta config across files is how
you end up with one job writing to the lake with a different committer than the
next one reads with.
"""
from __future__ import annotations

import os
import yaml
from pyspark.sql import SparkSession

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_CONFIG = os.path.join(REPO_ROOT, "conf", "recon_config.yml")

# Pinned for PySpark 3.5.1. delta-spark 3.1.0 targets Spark 3.5; hadoop-aws must
# match the Hadoop version Spark 3.5.1 is built against (3.3.4), and the AWS SDK
# must match hadoop-aws 3.3.4 (1.12.262). Mismatch here is the classic
# NoSuchMethodError / NoClassDefFoundError on first s3a write.
PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


def load_config(path: str = DEFAULT_CONFIG) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def build_spark(app_name: str, cfg: dict | None = None) -> SparkSession:
    cfg = cfg or load_config()
    s = cfg["spark"]
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", s["s3_endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", s["s3_access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", s["s3_secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # session-scoped UTC: the canonical schema stores UTC and nothing else
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
