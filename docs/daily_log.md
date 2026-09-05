
## Day 5 — Sat 5 Sep 2026 — point-in-time or it didn't happen

The T-5 proof holds. Fee schedule v2 took force on 9 July; rerunning 6 July after
that change reproduced the original output byte for byte —
efca74a30252c223d9a0aa5c61bb119b3924df1beb0c9fe7551aed71bc1d86a5 over 39,884
rows, identical to the baseline. The AS OF join selected v1 because
6 July is in [1 July, 9 July), so tolerances were identical, so matching was
identical, so the output was identical.

The negative control is what makes that a proof rather than a coincidence.
Forcing "always use today's fees" moves the hash to 9e890056...ce8420. Measured:
USD tolerance 310 -> 360, fee_residual or tolerance_applied differs on 19,814
rows, and 12 rows end up with a DIFFERENT counterpart — three dense ambiguity
blocks x two chain records x two sides. Break-class COUNTS do not move, because
every seeded AMOUNT_MISMATCH is corrupted by tolerance + [100,5000) and so sits
beyond both 310 and 360. What the wrong fee model breaks is the ranking, and it
breaks it exactly where the decision was close. That is the honest version and it
is a sharper story than a headline count moving.

Small thing I enjoyed: the naive negative control does not even run. Filtering
reference data to the current version while keeping the date predicate trips the
AS OF join's null assertion — a v2 row effective 9 July does not cover a 6 July
transaction. You have to deliberately delete the date predicate to make the wrong
thing execute, which is itself evidence the mechanism is load-bearing.

Delta time travel vs bitemporal validity: VERSION AS OF answers "what did the
table look like when we ran"; validity columns answer "what fee was in force on
the transaction's date". Reconciliation needs the second.

Two additions to the plan's DAG, both necessary. It stages the delivery locally
before the gate, because GE validates the FILE YOU WERE SENT including its
SHA256 and therefore has to see the actual bytes — that also keeps
run_checkpoint.py free of any cloud dependency, which is what lets CI run it
hermetically on ci_mini. And it canonicalises before it reconciles: the engine
reads canonical tables, so a DAG that jumped from the gate straight to the engine
would reconcile whatever was in the lake from the last manual run.

Nothing in the DAG imports pyspark. Every engine task shells out to the Spark
venv by absolute path — a process boundary, not an import, and the honest
production shape: the scheduler is not the runtime. Sensors are in reschedule
mode because three poke-mode sensors would hold LocalExecutor slots for six hours
and starve the pool.

Every task is idempotent, so the DAG is: sensors read, staging downloads, GE
reads, canonicalisation and the engine both write with replaceWhere scoped to
their own partition. Backfill is safe on any historical date.
