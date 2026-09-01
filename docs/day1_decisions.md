# Day 1 — Design Decisions

Eight decisions settled before a line of matching logic exists. Each one is a
thing that, if left open, would have surfaced as a mysterious failure on Day 2
or Day 3 and cost hours. Each is also an interview answer.

---

## D1 — Grain: `(source_system, txn_uid, leg)`

**Decision.** Both the answer key and the eventual `recon_output` table carry
one row per `(source_system, txn_uid, leg)` where `leg ∈ {PROCESSOR, BANK}`.

**Why.** Three-way reconciliation decomposes into two pairwise legs sharing one
spine. An internal row participates in **both** legs, so it has two expected
states, not one. The original contract's one-row-per-`(source, txn_uid)` grain
cannot express that. Worse, if the job unions leg-1 output with leg-2 output at
that grain, the internal spine is counted twice and the conservation assertion
can never balance — you would spend Day 2 debugging a control-total failure
that is a schema bug, not a lost record.

**Consequence.** `sum(canonical input for source S, currency C)` equals
`sum(output where source_system = S and leg = L, currency C)` for **each leg
independently**. Control totals assert per `(source_system, leg, currency)`.

---

## D2 — Ingestion window: internal is D, counterparties are D..D+2

**Decision.** A run for `--date D` reads:

| source | window |
|---|---|
| internal | `business_date = D` |
| processor | `business_date = D` |
| bank | `business_date BETWEEN D AND D + settlement_window_days` |

Output is written with `replaceWhere business_date = D`, keyed on the **internal
spine's** date. A bank row settling on D+2 belongs to the D partition because
the transaction it settles happened on D.

**Why.** `TIMING_DIFFERENCE` shifts settlement by +1/+2 days. If the run only
read `business_date = D` on every source, every timing difference would present
as `MISSING_IN_BANK` and the answer key would fail on Day 3 for a reason that
has nothing to do with matching. The generator enforces the other half of this:
**every internal row is on D**, and only bank rows move. Verified in the
fixtures — `internal dates: [2026-07-06]`, `bank dates: [2026-07-06, -07, -08]`.

**Consequence for Day 2.** The blocking dual-bucket fix is still required (a
Friday transaction settling Monday crosses the week bucket), but the date-window
question is now closed and the control-total frame is unambiguous: it is defined
over exactly the rows the run read, not over the whole canonical table.

---

## D3 — Tolerance parameters live in `conf/recon_config.yml`, read by both sides

**Decision.** `reference_data.fee_schedule` and `reference_data.fx_precision`
sit in the config. `scripts/seed_generator.py` and `scripts/seed_reference_data.py`
(Day 3) both read them. Nothing computes a fee or a tolerance from a literal.

**Why.** The generator has to corrupt `AMOUNT_MISMATCH` rows *beyond tolerance*,
and tolerance is a function of the fee schedule. If the generator invented its
own numbers, then on Day 3 the engine's derived tolerance would disagree with
the corruption magnitude, some mismatches would match inside tolerance, and the
precision assertion would fail against an answer key that was wrong all along.

**Values in force (v1).** `total_tolerance = max_fee_minor + fx_rounding_minor + epsilon`
→ USD 302 · EUR 302 · GBP 252 · JPY 402.
JPY's minor unit is the yen. A formula that assumes two decimals everywhere
produces 40200 for JPY, 100× too wide, and silently swallows real breaks. That
is `spark/tests/test_tolerance.py`'s case on Day 4.

---

## D4 — Two isolated virtual environments

**Decision.** `.venv-spark` for the engine, `.venv-airflow` for orchestration
(Day 5). Airflow never imports pyspark; it shells out to `spark-submit` with
absolute paths.

**Why.** PySpark 3.5 and Airflow 2.9 have incompatible pins on pendulum,
sqlalchemy and protobuf. One venv is a day lost to the pip resolver. A process
boundary is also the honest production shape: the scheduler is not the runtime.

**Corollary.** `great_expectations==0.18.19` is installed on **Day 2**, from
`requirements-ge.txt`, not on Day 1. It has the largest and most opinionated
dependency tree in the project. A GE resolver fight must not be able to block
the day the oracle gets built.

---

## D5 — Gross vs net: the bank amount is never grossed up

**Decision.** `canonicalize_bank` emits the bank's **net** deposit exactly as
reported. `canonicalize_processor` emits **gross** and drops `fee_minor` from
the canonical table.

**Why.** Canonicalization normalizes representation, never value. Grossing the
bank up using the processor's own fee column would reconcile the data to itself
and destroy the evidentiary value of every break the engine later reports. The
gross/net gap is absorbed by the tolerance *derived from versioned reference
data*, which a point-in-time rerun can reproduce (Day 5); a fixup inside a
mapper cannot be reproduced or audited.

**Consequence.** The bank leg is tolerant-by-design and the processor leg is
mostly exact. That asymmetry is the shape real fintech reconciliation has, and
it is why the resolver exists at all.

---

## D6 — The m:n trap asserts one-to-one stability, not a named pairing

**Decision.** Each trap group is two internal transactions that are
*indistinguishable* (same merchant, currency, week, amount, date) with two
settlements of the same net amount. The answer key sets
`trap_assert = ONE_TO_ONE_STABLE` rather than naming an intended counterpart.

**Why.** The original plan wanted the key to record "the intended pairing." It
cannot. Two identical transactions and two identical settlements carry no signal
that could distinguish them; an oracle demanding a specific pairing would be
asserting information the matcher provably does not have, and the only way to
pass would be to leak the answer into the matcher. The property that *is*
guaranteed, and the one finance actually needs, is: exactly one-to-one, no
fan-out, both matched, and the same assignment on every rerun. Density in these
blocks is exactly 2.0, which is *not* `> 2.0`, so they resolve on the greedy
path and test the stable tie-break rather than the fallback.

**Interview answer.** "I found my own trap was unfalsifiable — it demanded the
matcher know something the data doesn't contain — so I changed the assertion to
the property the system actually guarantees."

---

## D7 — Dense blocks are constructed so greedy provably loses, and the generator proves it

**Decision.** Each dense block is six *band* rows plus a two-row *chain*.

- **Band** — six transactions spaced 150 minor units apart with fees of 29–50.
  Because the fee is far smaller than the spacing, each true pair is also the
  nearest pair, so greedy and the optimal assignment agree. The band exists only
  to push candidate density above the threshold.
- **Chain** — two transactions positioned so a *wrong* edge (amount diff 100)
  outscores both true edges (diffs 166 and 174), while the second transaction's
  own settlement sits at diff 440, beyond the 302 tolerance, and is therefore
  not a candidate at all. Greedy takes the wrong edge first, strands the second
  transaction, and finishes the block one pair short. The optimal assignment
  takes both true edges because `1/167 + 1/175 > 1/101`.

Measured: **density 2.875, 23 candidate edges over 8 records, greedy 7 pairs,
optimal 8 pairs, optimal == truth.**

**Why it self-verifies.** `verify_dense_blocks()` rebuilds the exact candidate
set the engine will build, runs the same greedy rule and
`scipy.optimize.linear_sum_assignment`, and asserts all three properties before
a single CSV is written. A demo is only a demo if greedy loses; the generator
refuses to emit data where it doesn't.

---

## D8 — Container names are prefixed, and only one stack runs at a time

**Decision.** Compose project `recon`, containers `recon-minio`,
`recon-minio-init`, `recon-postgres-airflow`. Airflow's Postgres is published on
host port **5433**.

**Why.** Project 1 is complete and its compose file claims the container name
`minio` and host ports 9000/9001/5432. Docker refuses a duplicate container name
even when ports are free, and the port overlap would silently point this
project's s3a client at Project 1's object store. On 16GB, running both stacks
at once is not a resourcing accident to be discovered later — it is a documented
constraint.
