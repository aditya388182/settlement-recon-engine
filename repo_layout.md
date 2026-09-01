# Repository Layout — frozen for the whole build

Decided Day 1, unchanged through Day 6. Every one of the 55 tracked artifacts in
the execution plan has exactly one home below. Nothing gets relocated later,
because a path that moves on Day 4 breaks a CI workflow, an Airflow
`BashOperator`, a README link and three screenshots at once.

**The one rule:** no new top-level directory after today. If something does not
obviously belong in one of the twelve directories below, that is a signal the
module boundary is wrong, not that the tree needs another folder.

---

## 1. The tree

```
settlement-recon-engine/
├── .github/
│   └── workflows/
│       ├── ci.yml                          # [35] 4 jobs: unit, fixture gate, dag lint, ge+tf
│       └── deploy.yml                      # [36] package + stubbed S3 upload on main
│
├── airflow/
│   └── dags/
│       └── recon_daily.py                  # [32] sensors → GE gates → engine → metrics
│
├── conf/
│   ├── recon_config.yml                    # [3]  the single source of truth
│   └── recon_config.ci.yml                 # CI overlay: local filesystem paths, no MinIO
│
├── data/
│   ├── fixtures/
│   │   ├── ci_mini/                        # [12] COMMITTED. seed 7, 1k rows, 4 traps, 1 dense block
│   │   │   ├── answer_key_2026-07-06.csv
│   │   │   ├── bank_2026-07-06.csv
│   │   │   ├── control_line_2026-07-06.json
│   │   │   ├── internal_2026-07-06.csv
│   │   │   ├── manifest_2026-07-06.json
│   │   │   └── processor_2026-07-06.csv
│   │   ├── poisoned/                        # Day 5 GE-red demo. NEVER uploaded to landing/
│   │   │   └── bank_2026-07-09.csv         # truncated 30%
│   │   ├── answer_key_<date>.csv           # [8]  gitignored — regenerate from the seed
│   │   ├── bank_<date>.csv                 # [9]
│   │   ├── control_line_<date>.json
│   │   ├── internal_<date>.csv             # [9]
│   │   └── processor_<date>.csv            # [9]
│   └── manifests/
│       └── manifest_<date>.json            # [10] gitignored, regenerate with make_manifest.py
│
├── docs/
│   ├── screenshots/                        # [43-54] COMMITTED, 14 files, NN_snake_case.png
│   │   ├── 01_seed_stats.png               # Day 1
│   │   ├── 02_exact_match_pass.png         # Day 2
│   │   ├── 03_control_totals_pass.png      # Day 2
│   │   ├── 04_precision_recall_table.png   # Day 3
│   │   ├── 05_hungarian_trigger.png        # Day 3
│   │   ├── 06_skew_before.png              # Day 4
│   │   ├── 07_skew_after.png               # Day 4
│   │   ├── 08_byte_identity_pass.png       # Day 4
│   │   ├── 09_dropped_record_fail_closed.png # Day 4
│   │   ├── 10_t5_rerun_identical.png       # Day 5
│   │   ├── 11_airflow_green_and_ge_red.png # Day 5
│   │   ├── 12_grafana_break_trends.png     # Day 6
│   │   ├── 13_ci_pass.png                  # Day 6
│   │   └── 14_ci_fixture_fail.png          # Day 6
│   ├── proofs/                             # COMMITTED text captures the docs cite by name
│   │   ├── byte_identity_2026-07-06.txt    # Day 4: two hashes, equal
│   │   ├── dropped_record_2026-07-06.txt   # Day 4: violation output + untouched partition hash
│   │   └── t5_point_in_time_2026-07-06.txt # Day 5: baseline / rerun / negative-control hashes
│   ├── DAY1_EXECUTION_PLAN.md
│   ├── daily_log.md                        # [41] one section per day, war stories
│   ├── day1_decisions.md                   # the eight contracts settled before matching logic
│   ├── idempotency_and_point_in_time.md    # [40] five ingredients + T-5 proof, real hashes
│   ├── repo_layout.md                      # this file
│   └── video_script.md                     # [55] 5-7 min walkthrough
│
├── great_expectations/
│   ├── great_expectations.yml              # GE 0.18 filesystem context root
│   ├── expectations/                       # [33] GE 0.18 looks HERE, not in suites/
│   │   ├── bank_suite.json
│   │   ├── internal_suite.json
│   │   └── processor_suite.json
│   ├── checkpoints/
│   ├── uncommitted/                        # gitignored — GE writes validation results here
│   └── run_checkpoint.py                   # [34] --source --date, non-zero exit on failure
│
├── infra/
│   ├── docker-compose.yml                  # [2]  minio, minio-init, postgres, +Day6 observability
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   ├── break_trends.json           # [6]
│   │   │   └── run_health.json             # [6]
│   │   └── provisioning/
│   │       ├── dashboards/dashboards.yml   # tells Grafana to load the mounted JSON
│   │       └── datasources/prometheus.yml
│   └── prometheus/
│       └── prometheus.yml                  # [5]  scrapes pushgateway:9091 (service name)
│
├── runbooks/
│   ├── break_count_spike.md                # [38]
│   ├── overran_sla.md                      # [37]
│   └── tolerance_derivation_bug.md         # [39]
│
├── scripts/                                # host-python entrypoints: setup, data, proofs
│   ├── byte_identity_check.py              # [25] Day 4
│   ├── drop_record_test.py                 # [26] Day 4
│   ├── inject_skew.py                      # [27] Day 4 (thin wrapper over seed_generator)
│   ├── make_manifest.py                    # row counts + SHA256 → data/manifests/
│   ├── precision_recall.py                 # [24] THE harness; CI job 2 runs this exact file
│   ├── publish_metrics.py                  # Day 6, called by the DAG's publish_metrics task
│   ├── seed_generator.py                   # [7]  🔴 the oracle
│   ├── seed_reference_data.py              # [11] Day 3 v1, Day 5 v2 (bitemporal)
│   ├── setup_airflow.sh                    # [4]  Day 5, builds .venv-airflow
│   ├── upload_landing.py                   # fixtures + manifest → recon-landing/<date>/
│   └── verify_day1.py                      # Day 1 control-line assertion
│
├── spark/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   └── session.py                      # the ONE Spark session builder
│   ├── jobs/                               # spark-submit entrypoints, __main__ guarded
│   │   ├── __init__.py
│   │   ├── canonicalize_job.py             # Day 1 only
│   │   └── recon_job.py                    # [13] 🔴 the spine, Days 2-5
│   ├── recon/                              # importable library. NO side effects on import.
│   │   ├── __init__.py
│   │   ├── blocking.py                     # [15] block key + dual-bucket + block stats
│   │   ├── canonicalize.py                 # [14] 3 mappers, representation-only
│   │   ├── classifier.py                   # [20] 6 classes + evidence struct
│   │   ├── control_totals.py               # [21] conservation assertion, fails closed
│   │   ├── exact_match.py                  # [17] dedup routing + exact pass
│   │   ├── output.py                       # [23] replaceWhere + summary table
│   │   ├── resolver.py                     # [19] greedy + Hungarian, one module
│   │   ├── temporal.py                     # [22] AS OF join, half-open intervals
│   │   ├── tolerance.py                    # [16] derived per-currency tolerance
│   │   └── tolerant_match.py               # [18] candidate generation on residuals
│   └── tests/                              # pure in-memory pytest. No MinIO, no Postgres.
│       ├── __init__.py
│       ├── conftest.py                     # module-scoped local[*] SparkSession fixture
│       ├── test_classifier.py              # [30]
│       ├── test_control_totals.py          # [31]
│       ├── test_resolver.py                # [29]
│       └── test_tolerance.py               # [28]
│
├── terraform/                              # honest stub; `terraform plan` runs in CI job 4
│   ├── README.md                           # states plainly that this is a local stub
│   ├── main.tf
│   └── variables.tf
│
├── .gitignore                              # [1]
├── README.md                               # [42] written Day 6, 9 sections
├── requirements-ge.txt                     # installed Day 2, separately, on purpose
└── requirements-spark.txt                  # the .venv-spark pin set
```

Twelve top-level directories plus four root files. That is the whole surface.

---

## 2. Directory contracts

| Directory | Contract | Import rule |
|---|---|---|
| `spark/recon/` | One module per pipeline stage. Pure functions over DataFrames. No `argparse`, no file reads, no config loading, no side effects at import time. | Imported by `spark/jobs/`, `spark/tests/`, and `scripts/` |
| `spark/jobs/` | `spark-submit` entrypoints. Owns argument parsing, config loading, session construction, and the write. Every file guarded by `if __name__ == "__main__"`. | Imports `spark/recon/` and `spark/common/`. Never imported by `spark/recon/`. |
| `spark/common/` | Cross-cutting infrastructure with no reconciliation logic. Today that is exactly one file: the session builder. | Imported by everything |
| `spark/tests/` | In-memory pytest only. A test that needs MinIO, Postgres or a running job is in the wrong place — it belongs in `scripts/` as a proof. | Imports `spark/recon/` |
| `scripts/` | Host-python. Setup, data generation, and the *proof* harnesses that run the whole engine and assert on its output. | May import `spark/recon/` and `spark/common/` |
| `conf/` | Every tunable. No literal in any `.py` file may duplicate a value that lives here. | Read only through `load_config()` |
| `data/` | Generated. Only `ci_mini/` and `poisoned/` are committed. | — |
| `docs/` | Written artifacts and evidence. Screenshots and proof captures are committed; nothing here is generated at run time. | — |

The `spark/recon/` → `spark/jobs/` direction is the one that matters. It is why
the unit tests can import a resolver without starting a job, and why CI job 3
can `import airflow.dags.recon_daily` without pulling PySpark into the Airflow
venv.

---

## 3. Naming conventions — fixed

- **Dates in filenames:** ISO, suffixed. `internal_2026-07-06.csv`,
  `manifest_2026-07-06.json`. Never `20260706`, never a prefix.
- **Screenshots:** `NN_snake_case.png`, two digits, zero padded, numbered in
  capture order across the whole week. The README links them by number.
- **Tests:** `test_<module>.py` mirroring `spark/recon/<module>.py` one-for-one.
  A test file with no matching module means a module is missing.
- **Runbooks:** named for the *symptom*, not the cause. `overran_sla.md`, not
  `skew_fix.md` — the person opening it at 3am knows the symptom.
- **Landing zone keys:** `recon-landing/<date>/<source>_<date>.csv`. This is a
  contract with the Day-5 `S3KeySensor`, which polls
  `{{ ds }}/<source>_{{ ds }}.csv`.
- **Lake paths:** `recon-lake/canonical/<source>/`, `recon-lake/reference_data/`,
  `recon-lake/recon_output/`, `recon-lake/recon_summary/`. All from
  `conf/recon_config.yml` → `paths:`; never a literal `s3a://` string in a module.

---

## 4. What is committed and what is not

Committed:
- all code, config, workflows, runbooks, docs
- `data/fixtures/ci_mini/` — CI must run hermetically with no MinIO
- `data/fixtures/poisoned/` — the Day-5 GE-red demo needs a stable corrupt file
- `docs/screenshots/` and `docs/proofs/`

Ignored (already in `.gitignore`):
- `data/fixtures/*_<date>.csv`, `data/fixtures/control_line_*.json`,
  `data/manifests/*` — all regenerable from `--seed`, and a 10k-row fixture in
  git history is noise
- `.venv-spark/`, `.venv-airflow/`, `airflow_home/`, `great_expectations/uncommitted/`
- `spark-warehouse/`, `metastore_db/`, `derby.log`, `checkpoints/`, `*.jar`, `.ivy2/`

Add to `.gitignore` on Day 2 and Day 5 respectively:

```
great_expectations/uncommitted/
airflow_home/
```

---

## 5. Two structural deviations from the plan, and why

**`great_expectations/expectations/` instead of `great_expectations/suites/`.**
GE 0.18's filesystem `DataContext` reads its suite JSONs from a directory named
`expectations/` relative to `great_expectations.yml`. Naming it `suites/` means
either fighting the context config or building the context programmatically
every run. Use GE's own layout; the plan's inventory item [33] lives here.

**`conf/recon_config.ci.yml`.** CI job 2 runs the entire engine on `ci_mini/`
with local filesystem paths and no MinIO. The engine already takes paths from
config, so the clean expression of that is a second config file passed with
`--config`, not an environment-variable branch inside `recon_job.py`. Every
entrypoint in `spark/jobs/` and `scripts/` accepts `--config`, defaulting to
`conf/recon_config.yml`.

---

## 6. Scaffold the remaining directories now

Run once, from the repo root, so no empty directory has to be created mid-week:

```bash
for d in data/fixtures/poisoned docs/proofs \
         great_expectations/expectations great_expectations/checkpoints \
         great_expectations/uncommitted \
         infra/grafana/provisioning/dashboards \
         infra/grafana/provisioning/datasources; do
  mkdir -p "$d"; touch "$d/.gitkeep"
done
touch spark/tests/__init__.py
git add -A && git commit -m "chore: freeze repository layout for the full build"
```
