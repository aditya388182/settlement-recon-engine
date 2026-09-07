# Multi-Source Settlement Reconciliation Engine

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4?logo=databricks&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3A-C72E49?logo=minio&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?logo=apacheairflow&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18-FF6310)
![SciPy](https://img.shields.io/badge/SciPy-Hungarian-8CAAE6?logo=scipy&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A deterministic, explainable settlement reconciliation engine. It matches an internal ledger against a payment processor and a bank across two legs, resolves genuine ambiguity to a defensible one-to-one assignment, classifies every unmatched record into one of six break types with evidence attached, and **proves** — with commands you can run — that a rerun is a pure function of its input.

Project 1 had Postgres as ground truth. Here there is no external oracle, so the project manufactures one: a truth-first generator produces the answer key **before** the three messy source files are derived from it, and every correctness claim for six days is an assertion against that key.

> Local checkout is `settlement-recon-engine/`. Built and measured on a 16 GB M1 MacBook Air under Docker Compose. This is Project 2 from the six-day plan; the `internal` source is architecturally Project 1's Delta Change Data Feed.

---

## The claim this project actually defends

**Conservation is not correctness.**

A conservation check — every row that went in comes out exactly once, on counts and sums — is the safety net every reconciliation pipeline advertises. It is necessary and it is not sufficient, and this build demonstrates that empirically rather than rhetorically.

**Two real defects in this engine conserved money perfectly and were caught only by the answer key.**

| Defect | What conservation saw | What the answer key saw |
| :-- | :-- | :-- |
| The skew fix extended the blocking prefix into the unanchored tier, separating every pair whose references differ by construction | ✅ every row conserved | tier-3 assignments **2530 → 1528**; 40% of that population silently reclassified as breaks |
| Tolerant candidates were ranked on the raw amount difference, which on the bank leg **is the fee** | ✅ every row conserved | **10 wrong assignments, 20 false breaks**, recall 99.947% |

Neither one loses a record. Both produce a wrong ledger. So the engine carries two mechanisms, and they defend against different failures:

| Mechanism | Defends against | Implementation |
| :-- | :-- | :-- |
| **Control totals** | a record **lost** in processing | grain, then counts, then sums, per `(source_system, leg, currency)`, **before** publish; abort exit 1, partition untouched |
| **Answer key + harness** | a record **matched wrongly** | 14 assertions including `break_class == expected_class` on all 39,884 rows, run in CI on every PR |

Everything else in this README exists to make those two mechanisms trustworthy.

---

## Architecture Overview

```text
 scripts/seed_generator.py   ← truth table FIRST, then three derived views
        │  internal (gross) · processor (gross + itemised fee) · bank (NET deposit)
        │  + answer_key_<date>.csv  + control_line_<date>.json
        ▼
 recon-landing/<date>/  (MinIO, S3A)
        │
        ▼
 Airflow 2.9  recon_daily
   wait_internal ┐
   wait_processor├─► stage_delivery ─► ge_<src> ×3 ─► canonicalize ─► run_recon ─► publish_metrics
   wait_bank     ┘   (local staging)   (row band,     (delivery_date
                                        SHA256,        partition)
                                        schema)              │
                                                             ▼
 reference_data ──┐                              ┌──────────────────────────────┐
  bitemporal      │  AS OF join on business_date │  spark/jobs/recon_job.py     │
  [valid_from,    ├────────────────────────────► │                              │
   valid_to)      │                              │  route duplicate suspects    │
                  │                              │  block  (dual bucket)        │
                  │                              │  T1 exact  (block+ref+amt)   │
                  │                              │  T2 ref-anchored tolerant    │
                  │                              │  T3 amount+date tolerant     │
                  │                              │  resolve: greedy → Hungarian │
                  │                              │           → global reduce    │
                  │                              │  classify (6 + evidence)     │
                  │                              │  CONTROL TOTALS ─── abort ───┼──► exit 1
                  │                              │  replaceWhere publish        │
                  │                              └──────────────┬───────────────┘
                  │                                             ▼
                  │                       s3a://recon-lake/recon_output/   (partitioned by run date)
                  │                       s3a://recon-lake/recon_summary/  (date, leg, class, currency)
                  │                                             │
                  │        scripts/publish_metrics.py ──────────┘
                  │                    │
                  ▼                    ▼
        scripts/precision_recall.py   Pushgateway → Prometheus → Grafana
        scripts/byte_identity_check.py
        scripts/drop_record_test.py
```

Spark runs **on the host** as `local[*]`. Airflow runs **on the host** in its own virtualenv. Everything else is Docker Compose. Deliberate: PySpark 3.5 and Airflow 2.9 have incompatible pins on pendulum, sqlalchemy and protobuf, so they never share a Python process — Airflow shells out to the engine by absolute path. That is a process boundary, not an import, and it is also the honest production shape: the scheduler is not the runtime.

---

## Tech Stack

| Category | Technologies |
| :-- | :-- |
| **Programming** | Python 3.11 (`.venv-spark`; **not** 3.12 — pyspark 3.5.1 ships no 3.12 wheels) |
| **Processing** | PySpark 3.5.1, `applyInPandas` for per-block resolution |
| **Optimisation** | `scipy.optimize.linear_sum_assignment` (Hungarian fallback) |
| **Lakehouse** | Delta Lake 3.1.0 on MinIO (S3A), `replaceWhere` partition overwrite |
| **Reference data** | Bitemporal Delta table, half-open `[valid_from, valid_to)` intervals |
| **Orchestration** | Apache Airflow 2.9.3, LocalExecutor + Postgres 15 metadata DB |
| **Ingestion gate** | Great Expectations 0.18.19 plus a byte-level SHA256 manifest check |
| **Observability** | Prometheus Pushgateway, Prometheus 2.54, Grafana 11 (file-provisioned) |
| **CI/CD** | GitHub Actions — 4 jobs, hermetic fixture gate |
| **IaC** | Terraform 1.9 (documented stub, `local` provider — see limitations) |
| **Containerization** | Docker Compose on macOS / Apple silicon |

---

## Key Highlights

- Built a **truth-first oracle** that emits the answer key before the data, self-verifies its hardest construction with scipy, and **refuses to write** a fixture in which greedy already gets the right answer. A demo is only a demo if greedy loses.
- Achieved **100% recall on both legs, zero false matches**, and `break_class == expected_class` on **all 39,884 rows** — not a sample, every row.
- Implemented the ambiguity machinery real reconciliation needs: **deterministic greedy** (stable mergesort, dual-reference tie-break) with a **Hungarian fallback** above candidate density 2.0, then a **global cross-block reduce** the original design was missing.
- Proved **byte-identical reruns**: two runs → `5da8f782…628abba8` over 39,884 rows, with five ingredients enumerated and each one falsifiable.
- Proved **point-in-time correctness with a negative control**: after a fee schedule change effective 9 July, rerunning 6 July reproduced the original hash exactly; forcing "today's fees" moved the hash and changed **12 counterparts** in exactly the blocks where the decision was close.
- Proved **fail-closed** on all three assertions, including the one people forget: exit 1, the violation naming the currency and the exact delta, **and the existing partition hashing identically before and after**.
- Diagnosed and fixed **whale skew deterministically**: max block **3,142 → 265**, p99 **3,117 → 5**, with results provably unchanged — and discovered that the standard salting argument is false for the unanchored population.
- Separated **filtering from ranking** on the bank leg: the tolerance bounds the raw gross/net gap, the score ranks the residual the fee model cannot explain. That fix removed 20 false breaks conservation could never have seen.
- Shipped a **hermetic CI gate** that runs the entire engine on a committed 1,000-row fixture — no MinIO, no Postgres, no network — and goes red when a single `kind="mergesort"` token is deleted.

---

## Skills Demonstrated

- Record linkage at scale: blocking, candidate generation, assignment problems, and the failure modes of each
- Designing a ground truth when none exists, and recognising when your own oracle is unfalsifiable
- Bitemporal modelling and point-in-time joins, with a negative control rather than a happy path
- Idempotent lakehouse publishing (`replaceWhere`), and stating byte-identity precisely enough to defend it
- Conservation invariants that fail closed, and an honest account of what they cannot catch
- Deterministic skew mitigation that survives a byte-identity proof
- Orchestration with real process boundaries, and data-quality gates that gate ingestion rather than pretending to gate correctness
- Correctness governance in CI: a contract that runs on every PR and catches a one-token nondeterminism bug

---

## Scale & Load Characteristics

Measured on a 16 GB M1 MacBook Air, Spark `local[*]`, `spark.sql.shuffle.partitions=8`, Delta on MinIO.

| Metric | Value |
| :-- | :-- |
| **Baseline delivery (2026-07-06)** | 10,000 internal / 9,977 processor / 9,907 bank |
| **Output rows** | 39,884 at `(source_system, txn_uid, leg)` |
| **Answer key rows** | 39,884 — same grain, so they must agree exactly |
| **Seeded class mix** | 89.37% matched · 4.81% amount mismatch · 2.90% timing · 1.11% missing-in-processor · 0.93% missing-in-bank · 0.88% duplicate |
| **Exact pass (T1)** | 9,408 pairs |
| **Ref-anchored tolerant (T2)** | 9,062 candidates → 9,062 assignments |
| **Unanchored tolerant (T3)** | 941 candidates → 845 assignments (= every blinded bank row) |
| **Blocks / block size** | 87 blocks, max 153, p99 153 |
| **Candidate density** | max 2.875, **exactly 3 blocks** above the 2.0 threshold |
| **Derived tolerances** | USD 310 · EUR 310 · GBP 260 · JPY 410 (minor units) |
| **Skew date (2026-07-07)** | 30,000 rows, whale merchant at 40.5% of volume |
| **Max block, skew before → after** | 3,142 → 265 (p99 3,117 → 5) |
| **CI fixture** | 1,000 rows, 3,992 output rows, full engine runs hermetically |
| **Unit tests** | 39, all in-memory, no external services |
| **Engine wall time (baseline)** | ~2–3 minutes end to end on `local[*]` |

Single-machine, fixture scale. Sized to prove the mechanisms, not to saturate a cluster — see [Honest Limitations](#honest-limitations).

---

## Data Model

### `recon_output` — one row per `(source_system, txn_uid, leg)`

Delta path `s3a://recon-lake/recon_output/`, partitioned by `business_date`.

| Column | Notes |
| :-- | :-- |
| `business_date` | the **run** date, and the partition key |
| `row_business_date` | the source row's **own** date — a bank row may be D+2 |
| `source_system` | `INTERNAL` \| `PROCESSOR` \| `BANK` |
| `leg` | `PROCESSOR` \| `BANK` — the internal spine appears in both |
| `txn_uid`, `txn_ref`, `amount_minor`, `currency` | carried from canonical |
| `match_state` | `EXACT_MATCHED` \| `TOLERANT_MATCHED` \| `DUPLICATE_SUSPECT` \| `UNMATCHED` |
| `break_class` | the six-class verdict |
| `counterpart_txn_uid`, `counterpart_ref` | null iff unmatched |
| `evidence` | struct — see below |
| `block_key` | diagnostic: which block decided this row |
| `run_ts` | the one column allowed to differ between reruns |

**`evidence`** carries `amount_diff`, `fee_residual`, `date_diff`, `score`, `method` (`EXACT`/`GREEDY`/`HUNGARIAN`), `tier` (`T1`/`T2`/`T3`), `hungarian_cost`, `candidate_count`, `tolerance_applied`. Every non-`MATCHED` row has it, asserted.

```text
source_system   INTERNAL
leg             BANK
txn_uid         INT-0004821
break_class     TIMING_DIFFERENCE
counterpart_ref M014-B3f9a1c7
evidence        { amount_diff: 297, fee_residual: 3, date_diff: 2,
                  score: 0.0833, method: GREEDY, tier: T3,
                  hungarian_cost: null, candidate_count: 2,
                  tolerance_applied: 310 }
```

`amount_diff` is the raw gross/net gap the **tolerance filtered** on. `fee_residual` is what the fee model could not explain, and it is what the **score ranked** on. Separating those two is the fix described in the claim above.

### `recon_summary` and `reference_data`

- `recon_summary` — `(business_date, leg, break_class, currency, n, sum_amount_minor)`, same `replaceWhere` pattern, feeds Grafana.
- `reference_data` — bitemporal: `kind` (`FEE_SCHEDULE` / `FX_PRECISION`), `currency`, `max_fee_minor`, `rate_bps`, `fee_jitter_max`, `fx_rounding_minor`, `version`, `valid_from`, `valid_to`. Intervals half-open, asserted **non-overlapping and gapless** before every write.

---

## Critical Correctness Safeguards

Six rules are load-bearing. Every one of them looks fine in a demo and is wrong in production, and every one was learned by breaking it.

| # | Rule | Why it exists |
| :-: | :-- | :-- |
| **1** | Output grain is `(source_system, txn_uid, leg)` | The internal row participates in **both** legs and has two states. At one-row-per-`(source, txn_uid)` the union double-counts the spine and control totals can never balance. |
| **2** | A run owns a **delivery**, not a date range — canonical is partitioned by `delivery_date` | Reading bank rows by `business_date in [D, D+w]` looks equivalent while one date exists, and reaches into D+1's and D+2's deliveries the moment a second one does. It surfaced as 9,386 duplicated grain keys. |
| **3** | Tolerance **filters** the raw gap; the score **ranks** the fee residual | On the bank leg a true pair's amount difference *is* the fee, capped for ~98% of rows. Ranking on it makes a 50-unit coincidence outscore a legitimate 300-unit pair. |
| **4** | The adaptive prefix applies **only** to the ref-anchored passes | "A true pair shares the whole reference, so no prefix can split it" is true only for pairs that share a reference. Tier 3's pairs differ by construction; extending the prefix there lost 40% of them. |
| **5** | Control totals assert **grain first**, then counts, then sums | Offsetting errors survive both aggregates: drop one row worth 2,000 and duplicate another worth 2,000 and only uniqueness of the grain catches it. |
| **6** | The chaos drop removes a row from the **output**, not a pair from the pair set | Dropping a pair demotes the record to a residual, conservation still holds, the run stays green, and the sabotage proves nothing. A chaos run that exits 0 is itself a failure. |

---

## Quick Start & Verification

### 1. Prerequisites

- Docker Compose (24.x+, Compose v2)
- **Python 3.11**, not 3.12 — pyspark 3.5.1 has no 3.12 wheels and pip will build from source and fail
- **Java 17**, exported before every Spark process

```bash
brew install python@3.11 openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
             /Library/Java/JavaVirtualMachines/openjdk-17.jdk
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
java -version   # must print 17
```

Brew's `openjdk@17` is keg-only and invisible to `/usr/libexec/java_home` until that symlink exists. Without it PySpark picks whatever JDK macOS hands it and dies inside Arrow with an `IllegalAccessError`.

> **One stack at a time on 16 GB.** Project 1's compose file claims the container name `minio` and ports 9000/9001/5432. Stop it first:
> `docker compose -f ../payments-cdc-pipeline/infra/docker-compose.yml down`

```bash
cd settlement-recon-engine
python3.11 -m venv .venv-spark && source .venv-spark/bin/activate
pip install -r requirements-spark.txt      # GE is deliberately NOT in here
```

### 2. Start the data plane

```bash
cd infra && docker compose up -d && docker compose ps && cd ..
```

Expect `recon-minio`, `recon-postgres-airflow`, `recon-pushgateway`, `recon-prometheus`, `recon-grafana` healthy and `recon-minio-init` exited 0. Confirm buckets `recon-landing` and `recon-lake` at http://localhost:9001.

### 3. Generate the oracle and land it

```bash
python scripts/seed_generator.py --date 2026-07-06 --rows 10000 --seed 42 --out data/fixtures/
python scripts/make_manifest.py  --date 2026-07-06
python scripts/upload_landing.py --date 2026-07-06
```

The generator's own summary is the first proof:

```text
internal    10000 rows | EUR 2493r ... | GBP 2515r ... | JPY 2492r ... | USD 2500r ...
processor    9977 rows | ...
bank         9907 rows | ...
  MATCHED                 8937  (89.37%)
  AMOUNT_MISMATCH          481  ( 4.81%)
  MISSING_IN_PROCESSOR     111  ( 1.11%)
  MISSING_IN_BANK           93  ( 0.93%)
  DUPLICATE                 88  ( 0.88%)
  TIMING_DIFFERENCE        290  ( 2.90%)
m2m trap groups : 20 (each 2x2 candidates, density 2.0 = greedy path, assert ONE_TO_ONE_STABLE)
dense block     : DENSE-00 density=2.875 edges=23 records=8 greedy=7 pairs / optimal=8 pairs
bank ref blinding: 845/9907 bank rows carry a settlement ref
answer key rows : 39884 (grain: source_system x txn_uid x leg)
tolerances      : USD=310 EUR=310 GBP=260 JPY=410
ORACLE VERIFIED : greedy loses in all 3 dense block(s); optimal == truth   seed=42
```

### 4. Run the engine

```bash
python scripts/seed_reference_data.py  --version v1
python spark/jobs/canonicalize_job.py  --date 2026-07-06
python scripts/verify_day1.py          --date 2026-07-06     # canonical == control line
python spark/jobs/recon_job.py         --date 2026-07-06
```

Healthy run:

```text
[read] canonical/internal: 10000 rows in window
[pass1 exact:PROCESSOR] 9408 pair(s)
[pass2 tolerant-T2:BANK] 9062 candidate(s) -> 9062 assignment(s)
[pass3 density:BANK] blocks=87 max=2.875 p99=2.875 above_threshold=3
HUNGARIAN activated block=USD|2026-07-06|M023 density=2.875 pairs=23
[pass3 tolerant-T3:BANK] 941 candidate(s) -> 845 assignment(s)
[union] 39884 output rows at (source_system, txn_uid, leg) grain
evidence complete on all 39884 rows
CONTROL TOTALS PASS — 2026-07-06 — counts and sums conserved per (source_system, leg, currency)
```

### 5. Prove correctness

```bash
python spark/jobs/recon_job.py --date 2026-07-06 --force-greedy \
  --output-path s3a://recon-lake/recon_output_greedy
python scripts/precision_recall.py --date 2026-07-06 \
  --greedy-output-path s3a://recon-lake/recon_output_greedy
```

All fourteen rows must read `PASS`, ending in `C11 break_class == expected_class on every row (39884)`. The class × class matrix must be a clean diagonal.

### 6. Orchestrate it

```bash
bash scripts/setup_airflow.sh
source .venv-airflow/bin/activate && export AIRFLOW_HOME=$PWD/airflow_home
airflow scheduler                       # terminal 1
airflow webserver -p 8080               # terminal 2
```

### 7. Access the UIs

| Service | URL | Credentials | What to look at |
| :-- | :-- | :-- | :-- |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | Reconciliation → Break Trends, Run Health |
| **Prometheus** | http://localhost:9090 | — | `recon_*` gauges; target `pushgateway:9091` UP |
| **Pushgateway** | http://localhost:9091 | — | last push from `publish_metrics.py` |
| **MinIO** | http://localhost:9001 | `minioadmin` / `minioadmin` | buckets `recon-landing`, `recon-lake` |
| **Airflow** | http://localhost:8080 | set at `airflow users create` | DAG `recon_daily`, 10 tasks |
| **Spark UI** | http://localhost:4040 | — | stage page → task duration histogram (the skew demo) |

---

## How to Run Each Proof

Every claim in this README maps to a command. That is what makes the project auditable rather than assertable.

| Claim | Command |
| :-- | :-- |
| Six-class classification matches the answer key on every row | `python scripts/precision_recall.py --date 2026-07-06` |
| Canonical tables preserve value, not just shape | `python scripts/verify_day1.py --date 2026-07-06` |
| Two runs produce identical logical output | `python scripts/byte_identity_check.py --date 2026-07-06` |
| A lost record aborts the run and leaves the partition intact | `python scripts/drop_record_test.py --date 2026-07-06` |
| A T-5 rerun survives a fee-schedule change | `python scripts/seed_reference_data.py --version v2 --effective 2026-07-09` then `byte_identity_check.py` |
| …and that is the mechanism, not luck | `python spark/jobs/recon_job.py --date 2026-07-06 --ignore-point-in-time --output-path s3a://recon-lake/recon_output_nopit` |
| Greedy provably loses on the seeded dense blocks | `python spark/jobs/recon_job.py --date 2026-07-06 --force-greedy …` then C10 in the harness |
| Whale skew is fixed deterministically | `python scripts/inject_skew.py --date 2026-07-07 --rows 30000 --whale M007 --whale-share 0.40` |
| A truncated delivery never reaches the engine | `python great_expectations/run_checkpoint.py --source bank --date 2026-07-09 --fixtures data/fixtures/poisoned` |
| Blocking, matching, tolerance, resolver, classifier logic in isolation | `python -m pytest spark/tests/ -v` |
| The DAG imports cleanly with sensors in reschedule mode | CI job 3, `dag-lint` |

---

## Project Structure

```text
settlement-recon-engine/
├── .github/workflows/
│   ├── ci.yml                          # 4 jobs; job 2 is the hermetic fixture gate
│   └── deploy.yml                      # package + artifact (S3 step honestly stubbed)
├── airflow/dags/
│   └── recon_daily.py                  # 10 tasks: sensors → stage → GE → canonicalize → engine → metrics
├── conf/
│   ├── recon_config.yml                # single source of truth for every tunable
│   └── recon_config.ci.yml             # CI overlay: local filesystem paths, no MinIO
├── data/
│   ├── fixtures/ci_mini/               # COMMITTED — 1,000 rows, seed 7, 4 traps, 1 dense block
│   ├── fixtures/poisoned/              # COMMITTED — truncated bank delivery for the GE red run
│   └── manifests/                      # generated: row counts + SHA256 per file
├── docs/
│   ├── DAY1..DAY6_EXECUTION_PLAN.md    # six plans, each with verified outputs
│   ├── day1_decisions.md               # 26 numbered decisions, each with the failure it prevents
│   ├── idempotency_and_point_in_time.md# five ingredients + T-5 proof with real hashes
│   ├── repo_layout.md                  # frozen on day one
│   ├── daily_log.md                    # what broke, each day
│   ├── video_script.md                 # 6-minute walkthrough
│   ├── proofs/                         # hash captures the docs cite by name
│   └── screenshots/                    # 14 captured proofs
├── great_expectations/
│   ├── expectations/{internal,processor,bank}_suite.json
│   └── run_checkpoint.py               # SHA256 → row band → suite; exit code IS the Airflow gate
├── infra/
│   ├── docker-compose.yml              # minio, postgres, pushgateway, prometheus, grafana
│   ├── prometheus/prometheus.yml       # scrapes pushgateway:9091 (service name, not localhost)
│   └── grafana/{dashboards,provisioning}/
├── runbooks/
│   ├── overran_sla.md
│   ├── break_count_spike.md
│   └── tolerance_derivation_bug.md
├── scripts/
│   ├── seed_generator.py               # the oracle — self-verifies with scipy before writing
│   ├── seed_reference_data.py          # bitemporal v1/v2, interval invariant asserted
│   ├── make_manifest.py · upload_landing.py · fetch_landing.py
│   ├── verify_day1.py
│   ├── precision_recall.py             # THE harness; CI job 2 runs this exact file
│   ├── byte_identity_check.py · drop_record_test.py · inject_skew.py
│   ├── publish_metrics.py              # derives control_totals_ok from published evidence
│   └── setup_airflow.sh
├── spark/
│   ├── common/{session.py, io.py}      # the ONE session builder; paths + delivery window
│   ├── jobs/{canonicalize_job.py, recon_job.py}
│   ├── recon/                          # pure library: no argparse, no IO, no import side effects
│   │   ├── canonicalize.py  blocking.py  exact_match.py
│   │   ├── tolerance.py     temporal.py  tolerant_match.py
│   │   ├── resolver.py      classifier.py
│   │   └── control_totals.py output.py
│   └── tests/                          # 39 in-memory tests, no MinIO / Postgres / running job
├── terraform/                          # honest stub; `terraform plan` runs in CI
├── README.md
├── requirements-spark.txt
└── requirements-ge.txt                 # separate on purpose — biggest dependency tree in the project
```

The `spark/recon/` → `spark/jobs/` import direction is the one that matters. `recon/` is a pure library with no argument parsing and no side effects at import time; `jobs/` owns entrypoints, sessions and writes, and imports downward. That is what lets the unit tests import a resolver without starting a job, and lets CI's DAG-lint job import the Airflow module without pulling PySpark into the Airflow venv.

---

## What Broke, and How I Found It

These are the failures that actually cost hours. Every one was found by **running** the thing, not by reading it.

| Failure | Symptom | Root cause | Fix |
| :-- | :-- | :-- | :-- |
| **Skew fix destroyed tier 3** | assignments 2530 → 1528; **control totals still green** | 8-char prefix separated every pair whose references differ by construction | adaptive prefix applies only to ref-anchored passes (Safeguard #4) |
| **Systematic mis-ranking on the bank leg** | 10 wrong assignments, 20 false breaks, recall 99.947% | score used `1/(1+amount_diff)`; a true pair's diff **is** the capped fee | filter on the raw gap, rank on the fee residual (Safeguard #3) |
| **A second date corrupted the first** | control totals: 9,386 duplicated grain keys | read window `business_date in [D, D+w]` reached into later deliveries | partition canonical by `delivery_date`; a run owns its delivery (Safeguard #2) |
| **Sabotage test proved nothing** | `--chaos-drop-one` exited 0, run green | the drop removed a *pair*; the record fell into residuals and stayed conserved | drop from the output, after the union (Safeguard #6) |
| **Offsetting errors passed conservation** | counts and sums both unchanged despite a lost row | a sum-and-count check is blind to drop-one/duplicate-one of equal value | assert grain uniqueness first (Safeguard #5) |
| **My own m:n trap was unfalsifiable** | the oracle demanded a specific pairing of two identical transactions | the data carries no signal distinguishing them; only leaking the answer could pass | assertion changed to `ONE_TO_ONE_STABLE` — 1:1, no fan-out, stable across runs |
| **Dense blocks died when scoring was fixed** | with an exact fee model the residual is 0, so greedy always wins | genuine ambiguity requires **imperfect information** | generator emits 0–8 minor units of settlement jitter; tolerance gains a `fee_jitter_max` term |
| **Tier 3 fabricated matches** | 3 processor-leg pairs from pure amount coincidence | tier 3 ran against every residual, including rows whose reference resolves fine | `left_anti` against the internal reference set; tier 3 empties on the processor leg |
| **Bare `EOFError`, no traceback** | first `applyInPandas` killed the Python worker | a worker is a fresh process — it inherits the environment, not the driver's `sys.path` | repo root onto `PYTHONPATH` in `session.py` |
| **Bare `EOFError`, again** | `sun.misc.Unsafe … not available` buried in the JVM log | JDK 17+ closes the direct buffers Arrow needs; `spark-submit` adds `--add-opens`, `python job.py` does not | inject via `PYSPARK_SUBMIT_ARGS` before the gateway JVM launches |
| **Worker died silently on the UDF** | no message at all | int64 column against an `IntegerType` field; Arrow will not coerce | the UDF pins every dtype on the way out |
| **CI job 2 could not run** | `unrecognized arguments: --config` | `canonicalize_job.py` and `verify_day1.py` never took the flag the CI overlay needs | `--config` on every entrypoint |
| **The naive negative control would not execute** | `TemporalJoinError: 4 rows with null max_fee_minor` | a v2 row effective 9 July does not cover a 6 July transaction | *left as-is* — you must **deliberately** delete the date predicate to make the wrong thing run, which is itself evidence the mechanism is load-bearing |

---

## Key Engineering Decisions

Twenty-six numbered decisions live in [`docs/day1_decisions.md`](docs/day1_decisions.md). The ones that shaped the architecture:

| Decision | Rationale | Trade-off accepted |
| :-- | :-- | :-- |
| **Truth-first generator** | With no external oracle, the only honest ground truth is one you manufacture before deriving the data from it. | Every correctness claim is against synthetic data. The seeded break distribution is an assumption about the world, not a measurement of it. |
| **Grain `(source_system, txn_uid, leg)`** | The spine is in both legs and has two states. | Output has roughly 4× the rows of the largest source. |
| **A run owns a delivery** | You reconcile the file you were sent. Settlement lag is handled by the `date_diff` filter, not by a read window. | Canonical must be partitioned by `delivery_date`, which is not the date anyone would guess. |
| **Bank amounts are never grossed up** | Grossing up with the processor's own fee reconciles the data to itself and destroys the evidentiary value of every break. | The bank leg is tolerant-by-design, which is why the resolver exists at all. |
| **Two tiers, sequential, not one candidate set** | The reference is the strongest signal available; ignoring it when present is negligent. Sequential tiers stop a weak coincidence stealing a record that had a good ref-anchored counterpart. | Two passes and two candidate frames instead of one. |
| **Greedy first, Hungarian above density 2.0** | Below the threshold they agree in practice and greedy is O(C log C). Above it, greedy measurably strands residuals. | Per-block resolution assumes bounded block size; the block-size guard makes that assumption visible rather than removing it. |
| **Global cross-block reduce** | Dual-bucket blocking means two blocks can assign the same `txn_uid`; per-block one-to-one is only local. | Conservative — a record that loses the reduce becomes a break, never a wrong match. |
| **Deterministic adaptive prefix, never random salt** | Random salt can separate true tolerant candidates *and* is nondeterministic, which kills byte-identity. | The prefix cannot help the unanchored tier at all, because those pairs do not share a reference. |
| **Bitemporal reference data, never `UPDATE` in place** | An in-place edit silently rewrites history and makes every past reconciliation irreproducible. | Two versions of every row, and an interval invariant to maintain. |
| **Control totals before publish** | A violated run must leave the previous partition serving readers. | The assertion costs a full extra aggregation per run. |
| **`replaceWhere` scoped to the run date** | A rerun replaces exactly its own partition, by construction rather than by convention. | Requires two date columns in the schema, which reads odd until you know why. |
| **GE gates ingestion, never correctness** | "Did the file arrive whole?" and "did we get the right answer?" are different questions with different owners. | A green data-quality dashboard proves nothing about the ledger, and the README has to say so. |
| **Sabotage and negative controls are flags, not code edits** | A proof that requires editing code to reproduce is not a proof anyone else can run, and it cannot go into CI. | Two extra flags on the job that must never be set in production. |
| **`recon_control_totals_ok` is derived, not reported** | A gauge that only the success path pushes is blind exactly when it matters. | The metrics task must run with `trigger_rule="all_done"`. |

---

## Testing & Validation

### Unit tests — 39, hermetic

```bash
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python -m pytest spark/tests/ -v
```

| File | Pins |
| :-- | :-- |
| `test_blocking.py` | week-boundary recall on a real Saturday; adaptive prefix cannot split a true pair; block stats |
| `test_exact_match.py` | duplicate routing stable under input order; the fan-out `assert_one_to_one` catches; dual-bucket does not double-count |
| `test_tolerance.py` | derived tolerances; **JPY zero-decimal upper bound**; half-open intervals; a gap fails loudly |
| `test_resolver.py` | greedy determinism; dual-reference tie-break; density boundary 1.9/2.0/2.1; a 3×3 where greedy and Hungarian provably differ; the global reduce |
| `test_classifier.py` | one case per class; `AMOUNT_MISMATCH` before `MISSING_*`; evidence completeness and counterpart consistency |
| `test_control_totals.py` | pass; dropped record; **offsetting error caught only by the grain check**; per-currency isolation; legs checked independently |

A test that needs MinIO, Postgres or a running job is not a unit test — it is a proof, and it lives in `scripts/`.

### The harness — 14 assertions

```bash
python scripts/precision_recall.py --date 2026-07-06 \
  --greedy-output-path s3a://recon-lake/recon_output_greedy
```

```text
CHECK  ASSERTION                                            RESULT   OFFENDING
C1     answer key rows without an output row                PASS             0
C1     output rows without an answer key row                PASS             0
C2     exact recall, leg PROCESSOR (100.0000% of 18816)     PASS             0
C3     matched rows the key calls a break                   PASS             0
C4     duplicate routing (88 seeded copies)                 PASS             0
C5     zero exact matches on the bank leg                   PASS             0
C6     recall for MATCHED (100.0000% of 38050)              PASS             0
C6     recall for TIMING_DIFFERENCE (100.0000% of 580)      PASS             0
C7     TIMING_DIFFERENCE rows carry date_diff > 0 (580)     PASS             0
C8     trap groups resolve one-to-one in-group (20 groups)  PASS             0
C9     expects_hungarian rows decided by HUNGARIAN (48)     PASS             0
C10    greedy differs on greedy_differs rows (12)           PASS             0
C11    break_class == expected_class on every row (39884)   PASS             0
C12    non-MATCHED rows carry evidence                      PASS             0
PRECISION/RECALL HARNESS: ALL CHECKS PASS
```

### The two headline proofs

```bash
python scripts/byte_identity_check.py --date 2026-07-06
```

```text
run 1: sha256=5da8f782c3d2a2516bb7d8d7dbf984bf6a13085ccc0d11060ffd7363628abba8  rows=39884
run 2: sha256=5da8f782c3d2a2516bb7d8d7dbf984bf6a13085ccc0d11060ffd7363628abba8  rows=39884
BYTE-IDENTITY PASS
```

```bash
python scripts/drop_record_test.py --date 2026-07-06
```

```text
partition before sabotage: sha256=5da8f782...628abba8 rows=39884
[CHAOS] dropping INTERNAL/PROCESSOR/INT-0000000 (USD 389286 minor) after resolution, before union
ControlTotalViolation: money not conserved for 2026-07-06 —
  INTERNAL/PROCESSOR/USD delta_n=-1 delta_sum=-389286 minor — run aborted, nothing published
NOTHING WAS WRITTEN. The previous partition is untouched.
partition after sabotage:  sha256=5da8f782...628abba8 rows=39884
FAIL-CLOSED PASS
```

Byte-identity is defined over **logical content in canonical order**, never over physical files. Parquet names, row-group boundaries, task ordering and the Delta transaction log all differ run to run, and that is expected. Exactly one column is allowed to differ: `run_ts`.

### Point-in-time, with the negative control

Fee schedule v2 takes force on 9 July. Rerunning 6 July reproduces the original hash **exactly**, because the AS OF join selects v1 for a date in `[1 July, 9 July)`. Forcing "today's fees" diverges:

| | point-in-time | negative control |
| :-- | :-- | :-- |
| SHA256 | `5da8f782…628abba8` | `4502ee09…8e2706c` |
| USD `total_tolerance` | 310 | 360 |
| rows with a different `fee_residual` / `tolerance_applied` | — | **19,814** |
| rows with a different `break_class` or counterpart | — | **12** |
| break-class **counts** | unchanged | unchanged |

The counts do not move, and that is worth understanding rather than papering over: every seeded `AMOUNT_MISMATCH` is corrupted by `tolerance + [100, 5000)`, so it sits beyond 360 as well as beyond 310. What moves is the **ranking** — and in the three dense ambiguity blocks that shift changes the assignment. **Twelve rows** (three blocks × two chain records × two sides) end up with a different counterpart. The wrong fee model breaks the reconciliation exactly where the decision was close.

### CI — 4 jobs, fail-closed

| Job | Proves |
| :-- | :-- |
| `unit-tests` | 39 tests green |
| **`fixture-matcher-gate`** | the entire engine on the committed 1,000-row fixture: Hungarian activation, byte-identity, all 14 harness checks, fail-closed sabotage — **hermetically**, no MinIO, no Postgres |
| `dag-lint` | `DagBag.import_errors == {}`, 10 tasks, sensors in reschedule mode |
| `ge-and-terraform` | suites pass on good files, **fail on a truncated one**, `terraform plan` valid |

| Branch | Change | Gate |
| :-- | :-- | :-- |
| `docs/readme-touch` | README edit | all four jobs **green** — `docs/screenshots/13_ci_pass.png` |
| `demo/remove-mergesort` | delete `kind="mergesort"` in `greedy()` | fixture gate **red** on the byte-identity assertion — `docs/screenshots/14_ci_fixture_fail.png` |

One token. pandas' default quicksort is not stable, so equal-scored candidates resolve by memory layout. No human reviewer catches that; the byte-identity assertion does. **The demo branch is never merged — `main` stays green.**

---

## Observability Contract

| Series | Producer | Meaning |
| :-- | :-- | :-- |
| `recon_control_totals_ok` | `publish_metrics.py`, **derived** | **Severity 1.** 1 iff a `recon_output` partition exists for the date — and the engine writes it only after conservation passes |
| `recon_break_count{class,currency,leg}` | same | break rows by class; the Break Trends stack |
| `recon_break_amount_minor_total` | same | money sitting in a non-`MATCHED` state |
| `recon_run_duration_seconds` | same, from `--started-at` | wall clock against the 2-hour SLA line |
| `recon_hungarian_activations` | same | distinct blocks that used the fallback; a jump means the candidate set changed shape |
| `recon_rows_published` | same | 0 means the run aborted before publish |

Two things that are easy to get wrong, and are commented in the files:

- The Airflow task carries `trigger_rule="all_done"` so metrics are pushed even when the engine fails. A gauge only the success path pushes keeps its last value of `1` and pages nobody.
- `prometheus.yml` targets the **compose service name** `pushgateway:9091`. Inside the Prometheus container, `localhost` is Prometheus. The engine runs on the host and pushes to `localhost:9091` from the host side, reaching the same container through the published port.

Grafana dashboards are file-provisioned, and **every panel and every target names the datasource by uid** (`recon-prom`). UI exports routinely carry a `null` datasource that resolves fine interactively and to nothing when provisioned from a file.

---

## Screenshots

Files live in `docs/screenshots/`.

| File | Shows |
| :-- | :-- |
| `01_seed_stats.png` | Generator summary: class mix, trap inventory, `ORACLE VERIFIED` |
| `02_exact_match_pass.png` | Exact tier 100% recall / 100% precision against the answer key |
| `03_control_totals_pass.png` | First conservation PASS with the per-currency ledger |
| `04_precision_recall_table.png` | The six-class diagonal and all 14 checks |
| `05_hungarian_trigger.png` | Fallback activating on exactly the three seeded dense blocks |
| `06_skew_before.png` | Task-duration histogram: one straggler 10–50× the median |
| `07_skew_after.png` | Flat histogram after the deterministic prefix fix |
| `08_byte_identity_pass.png` | Two runs, identical SHA256 |
| `09_dropped_record_fail_closed.png` | Abort with the named currency and delta; partition unchanged |
| `10_t5_rerun_identical.png` | T-5 rerun hash and the negative-control hash side by side |
| `11_airflow_green_and_ge_red.png` | Green DAG run, and the poisoned delivery stopped at `ge_bank` |
| `12_grafana_break_trends.png` | Break-$ and break-count dashboards across backfilled dates |
| `13_ci_pass.png` | All four CI jobs green |
| `14_ci_fixture_fail.png` | Fixture gate red on the deleted `mergesort` token |

---

## Troubleshooting

| Issue | Likely cause | What to do |
| :-- | :-- | :-- |
| Bare `EOFError` from `pyspark/serializers.py`, no traceback | worker cannot import `spark.recon.*`, **or** JDK 17+ blocks Arrow's direct buffers | Both are handled in `session.py`. If it recurs, run the same job under `spark-submit`; if that works it is the module-opens path |
| `IllegalAccessError` inside Arrow | wrong JDK | `echo $JAVA_HOME` must point at 17 |
| `replaceWhere` throws on write | a row's `business_date` is not the run date | the schema carries `row_business_date` for exactly this reason |
| Control totals fail with thousands of duplicate grain keys | another delivery's rows are in the read | confirm canonical is partitioned by `delivery_date` |
| Byte-identity fails | nondeterminism | diff the two sorted outputs; the first differing row names it. Check `kind="mergesort"`, the dual-reference tie-break, and that no float reached a filter |
| Chaos run exits 0 | the drop hit the pair set, not the output | it must remove a row from the union |
| Tier-3 assignments fall after adding a hot counterparty | the adaptive prefix leaked into tier 3 | it must apply only to the ref-anchored passes |
| Hungarian fires on ordinary blocks | the candidate set is wrong, not the threshold | check that tier 3 excludes rows whose reference resolves |
| `TemporalJoinError: null max_fee_minor` | a gap in the reference intervals | re-run `seed_reference_data.py`; the printed table must cover every currency continuously |
| Airflow sensors hang forever | `endpoint_url` missing from the `minio_s3` connection | the hook is dialling real AWS |
| Airflow install fails on import inside pydantic / werkzeug | installed over a dirty Python | always a **fresh** `.venv-airflow` plus the constraints URL |
| Grafana panel: "Datasource not found" | provisioned JSON with a `null` datasource | every panel and target must name uid `recon-prom` |
| `pip install` builds pyspark from source | Python 3.12 | use 3.11 |

---

## Runbooks

Each is Symptom → Diagnostic → Decision tree → Action → Verify, and each uses **only metric and log names that exist in this project**.

- **[`runbooks/overran_sla.md`](runbooks/overran_sla.md)** — `recon_run_duration_seconds` over SLA → block-size WARN → Spark UI task histogram → salt list → redeploy → safe rerun. Its verify step includes the tier-3 assignment count, because that is the trap this project fell into.
- **[`runbooks/break_count_spike.md`](runbooks/break_count_spike.md)** — check GE for a late or partial file **first**; then `recon_control_totals_ok`; then which class spiked, with a cause table per class.
- **[`runbooks/tolerance_derivation_bug.md`](runbooks/tolerance_derivation_bug.md)** — breaks clustering at the tolerance line → inspect `reference_data` → fix with a **new interval, never in place** → verify that earlier dates still reproduce their original hashes.

Measured proof write-up: [`docs/idempotency_and_point_in_time.md`](docs/idempotency_and_point_in_time.md).

---

## Teardown

Stop compute, keep the lake:

```bash
pkill -f spark.jobs.recon_job || true
pkill -f "airflow scheduler" || true
pkill -f "airflow webserver" || true
cd infra && docker compose stop && cd ..
```

Destroy everything (fixtures are regenerable from `--seed`, so this is cheap):

```bash
cd infra && docker compose down -v && cd ..
```

---

## Honest Limitations

1. **Single machine, fixture scale.** 10,000 rows per delivery, 30,000 for the skew demo. The design targets high volume; nothing here has run on a cluster.
2. **The whale date is capped at ~30,000 rows.** Above roughly 40,000 with a 40% whale, the within-block candidate join hands one pandas frame ~36M pairs and a 16 GB machine OOMs. `inject_skew.py` warns above 40k. The real fix at scale is a different block key, not a longer prefix.
3. **Per-block resolution assumes bounded block size.** The block-size guard makes that assumption visible; it does not remove it.
4. **The adaptive prefix cannot help the unanchored tier.** It applies only to the ref-anchored passes, by necessity. If that tier ever became the skewed one, this fix would not touch it.
5. **The unanchored tier is amount-and-date matching and is not perfect.** It is a few percent of rows by design, and its accuracy depends on the fee model being a good predictor. Where the model is wrong, so is the ranking — which is exactly what the negative control demonstrates.
6. **The answer key is manufactured.** It is the only way to have ground truth at all, but the seeded break distribution is an assumption about the world rather than a measurement of it.
7. **`block_key` and `candidate_count` change when the salt list changes.** The full-row hash therefore moves across a config change even though every result column is provably identical. Byte-identity proper is the *same-config* rerun; the cross-config claim is stated on results only.
8. **Terraform is a documented stub.** The `local` provider renders the intended lake layout so `terraform plan` is a real check. A plan against AWS with no credentials would be theatre. Nothing has been applied.
9. **One stack at a time on 16 GB.** Project 1's compose file claims the same container names and ports.
10. **Bank-side amount mismatches cannot be distinguished from missing rows** when the bank reference is blinded, because there is no same-reference counterpart to compare against. The generator seeds amount mismatches on the processor side only, so the harness never exercises that gap.

---

## Project 1 Integration

The `internal` source is architecturally Project 1's Delta Change Data Feed —
`table_changes('transactions', start, end)` over the Payments CDC Ledger output. The CSV fixture stands in so this repository is runnable standalone and, more importantly, so the answer key can exist at all: you cannot manufacture ground truth for a stream you did not generate.

The swap is a reader change in `spark/jobs/canonicalize_job.py` — read the CDF instead of the landing CSV and keep `canonicalize_internal` unchanged — because the canonical schema was designed to be the contract between the two projects. Project 1's soft-delete tombstones map onto a live-rows-only read, which is the same filter its parity checker applies.

---

## Future Improvements

- [ ] Replace the amount-and-date tier with a fee-adjusted probabilistic scorer that learns the residual distribution per merchant rather than assuming the schedule is exact.
- [ ] Add a second block key for the unanchored tier keyed on an amount bucket, so the skew fix can reach that population too.
- [ ] Emit the candidate-density distribution as a histogram rather than a gauge, so a shift in the candidate set is visible *before* it changes an answer.
- [ ] Seed bank-side amount mismatches so limitation #10 becomes a tested path rather than a documented gap.
- [ ] Derive the CI fixture from the generator at build time rather than committing it, so the gate can never drift from the oracle.
- [ ] Wire the Project 1 CDF reader for real and run both projects end to end on the same synthetic ledger.
- [ ] Record the Delta `VERSION AS OF` in run metadata alongside the bitemporal join — belt and suspenders for forensic reruns.

---

## License

MIT. Synthetic payments data only — no real cardholder information is generated or stored.