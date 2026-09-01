"""canonicalize.py — three source mappers onto one canonical schema.

THE ONE RULE OF THIS MODULE
---------------------------
Representation changes are allowed: units, timezone, column names, types.
VALUE changes are forbidden. Grossing up the bank's net amount, netting the
processor's fee, or rounding anything here silently destroys the
reconciliation's evidentiary value — you would be reconciling the data to
itself. The gross/net gap is absorbed later by the DERIVED tolerance
(Day 3), which is auditable; a fixup here is not.

Canonical schema (shared contract):
    txn_uid       STRING     stable per-row id, unique within a source
    business_date DATE
    txn_ref       STRING     merchant-prefixed; a true pair shares the FULL ref
    amount_minor  BIGINT     integer minor units (JPY minor unit = the yen)
    currency      STRING     USD | EUR | GBP | JPY
    side          STRING     DEBIT | CREDIT
    source_system STRING     INTERNAL | PROCESSOR | BANK
    event_ts      TIMESTAMP  UTC
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

CANONICAL_COLS = ["txn_uid", "business_date", "txn_ref", "amount_minor",
                  "currency", "side", "source_system", "event_ts"]

# Raw CSV schemas. Amounts are read as STRING and cast to bigint explicitly —
# never inferSchema, which will happily hand you a double for a money column.
INTERNAL_SCHEMA = T.StructType([
    T.StructField("txn_uid", T.StringType()),
    T.StructField("business_date", T.StringType()),
    T.StructField("txn_ref", T.StringType()),
    T.StructField("amount_minor", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("side", T.StringType()),
    T.StructField("source_system", T.StringType()),
    T.StructField("event_ts", T.StringType()),
])
PROCESSOR_SCHEMA = T.StructType([
    T.StructField("txn_uid", T.StringType()),
    T.StructField("business_date", T.StringType()),
    T.StructField("txn_ref", T.StringType()),
    T.StructField("gross_amount_minor", T.StringType()),
    T.StructField("fee_minor", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("side", T.StringType()),
    T.StructField("source_system", T.StringType()),
    T.StructField("event_ts", T.StringType()),
])
BANK_SCHEMA = T.StructType([
    T.StructField("txn_uid", T.StringType()),
    T.StructField("business_date", T.StringType()),
    T.StructField("txn_ref", T.StringType()),
    T.StructField("net_amount_minor", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("side", T.StringType()),
    T.StructField("source_system", T.StringType()),
    T.StructField("event_ts", T.StringType()),
])
SOURCE_SCHEMAS = {"internal": INTERNAL_SCHEMA,
                  "processor": PROCESSOR_SCHEMA,
                  "bank": BANK_SCHEMA}


def _common(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("business_date", F.to_date("business_date", "yyyy-MM-dd"))
            .withColumn("event_ts",
                        F.to_utc_timestamp(
                            F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"), "UTC")))


def canonicalize_internal(df: DataFrame) -> DataFrame:
    """System of record. Gross amount, all rows, the spine of both legs."""
    return (_common(df)
            .withColumn("amount_minor", F.col("amount_minor").cast("bigint"))
            .withColumn("side", F.lit("DEBIT"))
            .withColumn("source_system", F.lit("INTERNAL"))
            .select(*CANONICAL_COLS))


def canonicalize_processor(df: DataFrame) -> DataFrame:
    """Processor reports GROSS plus an itemized fee.

    The canonical amount is the GROSS column. fee_minor is deliberately dropped
    from the canonical table: it is reference-ish data about the transaction,
    and if the matcher were allowed to see the processor's own fee it would be
    reconciling the processor against itself. The tolerance comes from the
    versioned reference_data table instead (Day 3/Day 5), which is the thing a
    point-in-time rerun can reproduce.
    """
    return (_common(df)
            .withColumn("amount_minor", F.col("gross_amount_minor").cast("bigint"))
            .withColumn("side", F.lit("DEBIT"))
            .withColumn("source_system", F.lit("PROCESSOR"))
            .select(*CANONICAL_COLS))


def canonicalize_bank(df: DataFrame) -> DataFrame:
    """Bank reports the NET deposit (gross - fee).

    DO NOT gross this up. The net is what the bank actually says; the fee-derived
    tolerance absorbs the difference on the bank leg. This is exactly why the
    bank leg is tolerant-by-design while the processor leg is mostly exact.
    """
    return (_common(df)
            .withColumn("amount_minor", F.col("net_amount_minor").cast("bigint"))
            .withColumn("side", F.lit("CREDIT"))
            .withColumn("source_system", F.lit("BANK"))
            .select(*CANONICAL_COLS))


_DISPATCH = {"internal": canonicalize_internal,
             "processor": canonicalize_processor,
             "bank": canonicalize_bank}


def canonicalize(df: DataFrame, source: str) -> DataFrame:
    if source not in _DISPATCH:
        raise ValueError(f"unknown source {source!r}; expected one of {list(_DISPATCH)}")
    return _DISPATCH[source](df)


# ---------------------------------------------------------------------------
# DECIMAL-STRING DEFENCE (not exercised by today's fixtures — they are integer)
# ---------------------------------------------------------------------------
# If a source ever arrives with decimal-string amounts ("49.99"), parse them as
#
#     from decimal import Decimal
#     @F.udf(returnType=T.LongType())
#     def to_minor(s, scale):
#         return int(Decimal(s) * (10 ** scale))
#
# and NEVER cast("double"). A double cast of "49.99" gives 49.990000000000002,
# which multiplied by 100 and truncated becomes 4998 — a one-cent break invented
# by the parser. The scale comes from reference_data.fx_precision.decimals, so
# JPY (0 decimals) is handled by the same code path rather than a special case.
