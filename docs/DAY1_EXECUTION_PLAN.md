# Project 2 — Day 1 Execution Plan (Tuesday, 1 September 2026)
### Multi-Source Settlement Reconciliation Engine · Pre-Stage + Stage 0 Part 1

**Daily theme: "The answer key is the product."**

By tonight: three deliberately messy source files sit in the MinIO landing zone
with a machine-readable answer key describing every seeded break and trap, plus
three canonical Delta tables in integer minor units that reproduce the
generator's own control line to the minor unit. No matching logic exists yet.

**Fixture dates stay `2026-07-06`.** That is a parameter of the data, not
today's date. Do not "helpfully" change it — every screenshot, manifest and
`replaceWhere` in the next five days keys off it.

---

## 0. READ THIS FIRST — what is different from the original 6-day plan

The original Day 1 budgeted two hours to write `seed_generator.py` from scratch.
That file is the keystone: every proof from Day 2 to Day 6 asserts against it,
and the hardest part of it (constructing a block where greedy *provably* loses
to the optimal assignment) is a small optimisation problem that does not
reliably fall out in two hours.

**It is already written and sandbox-verified.** So is the config, the
canonicalisation layer, the manifest generator, the upload script and the Day-1
verification harness. Your Day 1 is now:

1. Stand the infrastructure up.
2. Run the oracle and **verify it yourself** rather than trusting it.
3. Read the code closely enough to defend every design decision out loud.
4. Get canonical tables on MinIO that provably preserve value.

That is a better use of the day than typing, and it protects the one artifact
that, if wrong, makes every green check for the rest of the week a lie.

### Verification status — be precise about this in interviews

| Component | Status |
|---|---|
| `seed_generator.py` — determinism (two runs, byte-identical) | **Verified** — `diff -r` clean, SHA256s recorded below |
| `seed_generator.py` — class distribution vs spec rates | **Verified** — 5.10% / 0.89% / 0.82% / 0.94% / 3.04% |
| `seed_generator.py` — dense blocks defeat greedy | **Verified** — scipy: density 2.875, greedy 7 pairs, optimal 8, optimal == truth |
| `seed_generator.py` — every `AMOUNT_MISMATCH` beyond tolerance | **Verified** — 510 rows, 0 inside tolerance |
| `seed_generator.py` — every true bank pair inside tolerance | **Verified** — 0 rows outside |
| `make_manifest.py`, `upload_landing.py` (arg parsing, hashing) | **Verified** for hashing; upload path unverified (no MinIO in sandbox) |
| `canonicalize.py`, `session.py`, `canonicalize_job.py`, `verify_day1.py` | **Syntax-checked only.** No Spark, no Delta jars, no MinIO in the sandbox. Treat Blocks 1.4 and 1.6 as real debugging work. |

---

## 1. PRE-FLIGHT — do this before 09:00 (20 min)

Run every line. Do not proceed with a red check.

```bash
# 1. Shut Project 1 down. Its compose file claims container name `minio` and
#    host ports 9000/9001/5432. Docker will refuse a duplicate name even if the
#    ports are free, and the port overlap would silently point this project's
#    s3a client at Project 1's object store.
docker compose -f ~/path/to/payments-cdc-pipeline/infra/docker-compose.yml down
docker ps            # expect: no containers, or none named minio/postgres

# 2. Ports must be free
for p in 9000 9001 5433; do
  echo -n "port $p: "; lsof -nP -iTCP:$p -sTCP:LISTEN >/dev/null 2>&1 \
    && echo "IN USE — stop that process" || echo free
done

# 3. Toolchain
docker --version                 # 24.x+
docker compose version           # v2.x
git --version
```

**Python 3.11 — not 3.12.** PySpark 3.5.1 ships no 3.12 wheels; pip will build
from source, fail, and eat an hour.

```bash
brew install python@3.11 openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
             /Library/Java/JavaVirtualMachines/openjdk-17.jdk
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc

python3.11 --version             # 3.11.x
java -version                    # openjdk version "17.x"
```

- [ ] Project 1 stack down, no name or port collisions
- [ ] Ports 9000, 9001, 5433 free
- [ ] Docker 24+, Compose v2
- [ ] Python 3.11 present
- [ ] Java 17 present and `JAVA_HOME` exported **in this shell and in `~/.zshrc`**

> **Pitfall.** Brew's `openjdk@17` is keg-only and invisible to
> `/usr/libexec/java_home` until the symlink above exists. Without it PySpark
> picks whatever JDK macOS hands it (often 21) and dies on an obscure
> `IllegalAccessError` deep in Arrow. **Countermeasure:** the symlink line is
> not optional; re-run `java -version` after it and confirm 17.

---

## 2. TIME-BLOCKED SCHEDULE

| Time | Block | Focus |
|---|---|---|
| 09:00–09:30 | 1.1 | Repo, venv, drop in the provided files |
| 09:30–10:15 | 1.2 | Docker stack + MinIO buckets |
| 10:15–10:30 | | ☕ break |
| 10:30–12:15 | 1.3 | **The oracle: run it, verify it, read it** 🔴 largest block |
| 12:15–13:00 | | 🍴 lunch |
| 13:00–14:15 | 1.4 | First Spark session + Delta + s3a smoke test 🔴 |
| 14:15–15:00 | 1.5 | Manifests + upload to the landing zone |
| 15:00–15:15 | | ☕ break |
| 15:15–16:30 | 1.6 | Canonicalize job + control-line assertion 🔴 |
| 16:30–17:15 | 1.7 | CI mini-fixture, screenshot 01, defence drill |
| 17:15–18:00 | 1.8 | Buffer / debugging overflow |
| 18:00–18:30 | 1.9 | Review, daily log, commit |

**Golden rule.** If a block runs over by more than 45 minutes, stop, write where
you are in `docs/daily_log.md`, and move on. Block 1.8 exists for exactly this.

---

## BLOCK 1.1 (09:00–09:30) — Repo + venv

```bash
cd ~/projects                       # wherever Project 1 lives, alongside it
mkdir -p settlement-recon-engine && cd settlement-recon-engine && git init

for d in infra/prometheus infra/grafana/dashboards conf \
         spark/jobs spark/recon spark/common spark/tests \
         scripts data/fixtures/ci_mini data/manifests \
         airflow/dags great_expectations/suites \
         .github/workflows runbooks docs/screenshots terraform; do
  mkdir -p "$d"
done
touch spark/__init__.py spark/common/__init__.py spark/recon/__init__.py \
      spark/jobs/__init__.py docs/daily_log.md
```

Unzip the provided starter over this tree (it contains exactly these paths).
Then:

```bash
python3.11 -m venv .venv-spark
source .venv-spark/bin/activate
python --version                    # MUST print 3.11.x
pip install --upgrade pip
pip install -r requirements-spark.txt
```

```bash
# scipy is the Hungarian fallback on Day 3. Verify it NOW, not Thursday.
python -c "from scipy.optimize import linear_sum_assignment; print('hungarian ok')"
python -c "import pyspark, delta, pandas, pyarrow, numpy, boto3, yaml; print('imports ok')"
```

- [ ] Directory tree created
- [ ] Starter files in place
- [ ] `.venv-spark` on Python **3.11**
- [ ] `hungarian ok` and `imports ok`
- [ ] `git add -A && git commit -m "scaffold: repo structure, config, isolated spark venv"`

> **Note.** `great_expectations` is deliberately **not** in
> `requirements-spark.txt`. It goes in on Day 2 from `requirements-ge.txt`. Its
> dependency tree is the most likely thing in this project to fight the pins
> above, and a resolver fight must not be able to block the day the oracle gets
> built. **Countermeasure if it does fight on Day 2:** put GE in its own venv
> and call `run_checkpoint.py` by absolute path — the same process-boundary
> pattern Airflow uses on Day 5.

---

## BLOCK 1.2 (09:30–10:15) — Docker stack

```bash
cd infra && docker compose up -d && docker compose ps
```

Expected: `recon-minio` healthy, `recon-postgres-airflow` healthy,
`recon-minio-init` exited 0.

```bash
docker compose logs minio-init | tail -20    # expect both buckets listed
```

Open <http://localhost:9001> → log in `minioadmin` / `minioadmin` → confirm
buckets **recon-landing** and **recon-lake** exist.

```bash
cd .. && python -c "
import boto3, yaml
from botocore.client import Config
cfg = yaml.safe_load(open('conf/recon_config.yml'))['spark']
s3 = boto3.client('s3', endpoint_url=cfg['s3_endpoint'],
                  aws_access_key_id=cfg['s3_access_key'],
                  aws_secret_access_key=cfg['s3_secret_key'],
                  config=Config(signature_version='s3v4'), region_name='us-east-1')
print([b['Name'] for b in s3.list_buckets()['Buckets']])"
```

Expected: `['recon-lake', 'recon-landing']`

- [ ] Both containers healthy
- [ ] Both buckets visible in the console **and** from boto3
- [ ] Postgres reachable: `docker exec recon-postgres-airflow pg_isready -U airflow`

> **Pitfall.** On Apple silicon, if `minio-init` exits non-zero with a
> connection refused, the healthcheck raced. **Countermeasure:**
> `docker compose up -d minio-init` again — it is idempotent (`mc mb -p`).

---

## BLOCK 1.3 (10:30–12:15) — 🔴 The oracle

This is the highest-leverage block of the entire week. Half of it is running,
half is reading.

### 1.3a — Generate (10 min)

```bash
python scripts/seed_generator.py --date 2026-07-06 --rows 10000 --seed 42 \
  --out data/fixtures/
```

Expected output, exactly:

```
========================================================================
SEED GENERATOR — date=2026-07-06 seed=42 rows=10000
========================================================================
internal    10000 rows | EUR 2514r 620507492m | GBP 2453r 617016859m | JPY 2454r 607110594m | USD 2579r 636555711m
processor   10005 rows | EUR 2506r 618148435m | GBP 2455r 617352896m | JPY 2455r 607409772m | USD 2589r 639532222m
bank         9918 rows | EUR 2490r 613392245m | GBP 2433r 610348487m | JPY 2437r 602191247m | USD 2558r 630689675m
------------------------------------------------------------------------
seeded classes (per truth txn):
  MATCHED                 8921  (89.21%)
  AMOUNT_MISMATCH          510  ( 5.10%)
  MISSING_IN_PROCESSOR      89  ( 0.89%)
  MISSING_IN_BANK           82  ( 0.82%)
  DUPLICATE                 94  ( 0.94%)
  TIMING_DIFFERENCE        304  ( 3.04%)
------------------------------------------------------------------------
m2m trap groups : 20 (each 2x2 candidates, density 2.0 = greedy path, assert ONE_TO_ONE_STABLE)
dense block     : DENSE-00 density=2.875 edges=23 records=8 greedy=7 pairs / optimal=8 pairs
dense block     : DENSE-01 density=2.875 edges=23 records=8 greedy=7 pairs / optimal=8 pairs
dense block     : DENSE-02 density=2.875 edges=23 records=8 greedy=7 pairs / optimal=8 pairs
answer key rows : 39923 (grain: source_system x txn_uid x leg)
tolerances      : USD=302 EUR=302 GBP=252 JPY=402
ORACLE VERIFIED : greedy loses in all 3 dense block(s); optimal == truth   seed=42
========================================================================
```

**📸 `docs/screenshots/01_seed_stats.png`** — capture this block.

If a single number differs, stop. Either the config was edited or the Python
version changed `random`'s behaviour; do not proceed on a different oracle.

- [ ] Output matches character for character
- [ ] Screenshot 01 captured

### 1.3b — Verify determinism yourself (10 min)

Do not take the generator's word for it.

```bash
python scripts/seed_generator.py --date 2026-07-06 --rows 10000 --seed 42 --out /tmp/r1/ >/dev/null
python scripts/seed_generator.py --date 2026-07-06 --rows 10000 --seed 42 --out /tmp/r2/ >/dev/null
diff -r /tmp/r1 /tmp/r2 && echo "DETERMINISM: byte-identical"
shasum -a 256 data/fixtures/*_2026-07-06.*
```

Reference SHA256s (seed 42, 10000 rows):

```
543fb1dba395fd1a2ecdfb12fdfaa86762b1e4284883c6d71f847c140bd326a6  answer_key_2026-07-06.csv
4fee214ae09bffd02ee22c98776bb03646ef9ed33d2807d64a2f87fea6299ac0  bank_2026-07-06.csv
16008ec226a6d606fb419b22f2354478549acebd58862345c2a10e4fe853dc43  control_line_2026-07-06.json
a2a5bcee56727923334a5ad68e8d6d2809ec862f34dc7e1bdaa957f757bfbd25  internal_2026-07-06.csv
766c0b4e9b5b5e321306f8dc1605cb0790e57a8cba9747161085dc8b54b88119  processor_2026-07-06.csv
```

- [ ] `diff -r` clean
- [ ] All five hashes match the reference

> If the hashes differ but `diff -r` is clean, you are on a different Python
> minor version — `random.Random` is stable across 3.9–3.12 for the methods used
> here, so the more likely cause is an edited `conf/recon_config.yml`.
> **Countermeasure:** `git diff conf/recon_config.yml`.

### 1.3c — Audit the fixtures against the contract (20 min)

Paste and run:

```bash
python - <<'EOF'
import csv, yaml, collections
cfg = yaml.safe_load(open("conf/recon_config.yml"))
def fee(g, c):
    s = cfg["reference_data"]["fee_schedule"][c]
    return min(g * s["rate_bps"] // 10000, s["max_fee_minor"])
def tol(c):
    rd = cfg["reference_data"]
    return (rd["fee_schedule"][c]["max_fee_minor"]
            + rd["fx_precision"][c]["fx_rounding_minor"] + rd["epsilon_minor"])
d = "data/fixtures/"
I = {r["txn_ref"]: r for r in csv.DictReader(open(d+"internal_2026-07-06.csv"))}
P = list(csv.DictReader(open(d+"processor_2026-07-06.csv")))
B = list(csv.DictReader(open(d+"bank_2026-07-06.csv")))
K = list(csv.DictReader(open(d+"answer_key_2026-07-06.csv")))
puid = {r["txn_uid"]: r for r in P}

mm = [k for k in K if k["expected_class"]=="AMOUNT_MISMATCH" and k["source_system"]=="PROCESSOR"]
bad = sum(1 for k in mm
          if abs(int(puid[k["txn_uid"]]["gross_amount_minor"])
                 - int(I[puid[k["txn_uid"]]["txn_ref"]]["amount_minor"]))
             <= tol(puid[k["txn_uid"]]["currency"]))
print("AMOUNT_MISMATCH rows:", len(mm), "| inside tolerance (must be 0):", bad)

refs = collections.Counter(r["txn_ref"] for r in P)
dup = [k for k in K if k["expected_class"]=="DUPLICATE"]
print("DUPLICATE key rows:", len(dup),
      "| processor refs appearing >1:", sum(1 for v in refs.values() if v>1),
      "| distinct uids:", len({k['txn_uid'] for k in dup})==len(dup))

out = sum(1 for b in B if b["txn_ref"] in I and
          abs(int(I[b["txn_ref"]]["amount_minor"]) - int(b["net_amount_minor"])) > tol(b["currency"]))
print("bank rows outside tolerance of their internal (must be 0):", out)

print("internal dates:", sorted({r['business_date'] for r in I.values()}))
print("bank dates:", sorted({r['business_date'] for r in B}))
g = collections.Counter((k["source_system"], k["txn_uid"], k["leg"]) for k in K)
print("answer-key grain unique:", all(v==1 for v in g.values()), "| rows:", len(K))
print("expects_hungarian:", sum(1 for k in K if k["expects_hungarian"]=="true"),
      "| greedy_differs:", sum(1 for k in K if k["greedy_differs"]=="true"))
EOF
```

Expected:

```
AMOUNT_MISMATCH rows: 510 | inside tolerance (must be 0): 0
DUPLICATE key rows: 94 | processor refs appearing >1: 94 | distinct uids: True
bank rows outside tolerance of their internal (must be 0): 0
internal dates: ['2026-07-06']
bank dates: ['2026-07-06', '2026-07-07', '2026-07-08']
answer-key grain unique: True | rows: 39923
expects_hungarian: 48 | greedy_differs: 12
```

- [ ] All six assertions match

Each line is load-bearing:
- **0 mismatches inside tolerance** — otherwise Day 3's precision assertion
  fails against an answer key that was wrong.
- **bank rows all inside tolerance** — otherwise Day 3's recall collapses and
  you will blame the matcher.
- **internal on D only, bank spanning D..D+2** — this is decision **D2**, the
  ingestion window, made concrete.
- **48 / 12** — 3 dense blocks × 8 records × 2 rows (internal bank-leg + bank)
  = 48; chain rows 3 × 2 × 2 = 12.

### 1.3d — Read the code (45 min) 🔴

Not optional. Open `scripts/seed_generator.py` and `docs/day1_decisions.md`
side by side and be able to answer, unaided:

1. Why is the answer key grained by `(source_system, txn_uid, leg)` and not by
   `(source_system, txn_uid)`? *(D1 — the internal spine is in both legs; at the
   wrong grain control totals double-count it and can never balance.)*
2. Where does the `AMOUNT_MISMATCH` corruption magnitude come from, and what
   breaks if the generator hardcodes it? *(D3)*
3. In `build_dense_blocks`, what is the band for and what is the chain for?
   Why does greedy lose? *(D7 — write the three diffs 100 / 166 / 174 and the
   inequality `1/167 + 1/175 > 1/101` on paper.)*
4. Why does the trap group assert `ONE_TO_ONE_STABLE` instead of a named
   counterpart? *(D6 — this is the strongest story on the whole day.)*
5. Why is there no float in this file outside `verify_dense_blocks()`?

- [ ] All five answered out loud without looking

---

## BLOCK 1.4 (13:00–14:15) — 🔴 First Spark session

The first `SparkSession` resolves three jars from Maven Central over Ivy. It is
slow once and instant afterwards, and it is the single most likely place for the
day to stall. Do it as its own step, before any real work depends on it.

```bash
python -c "
import sys; sys.path.insert(0, '.')
from spark.common.session import build_spark
spark = build_spark('smoke')
print('spark', spark.version)
spark.range(5).write.format('delta').mode('overwrite').save('s3a://recon-lake/_smoke/')
print('delta rows on minio:', spark.read.format('delta').load('s3a://recon-lake/_smoke/').count())
spark.stop()"
```

Expected: `spark 3.5.1` then `delta rows on minio: 5`. Confirm a `_smoke/`
prefix appeared in the **recon-lake** bucket in the MinIO console, then delete it
from the console.

- [ ] Spark session builds
- [ ] Delta write **and** read against MinIO succeed
- [ ] `_smoke/` cleaned up

> **Risk register for this block.**
> - *Ivy cannot reach Maven Central.* → `~/.ivy2` fills slowly or errors.
>   **Countermeasure:** check the network, then re-run; partial Ivy caches are
>   resumed, not corrupted. If a corporate proxy blocks it, pre-download the
>   three jars and use `spark.jars` with local paths instead of `spark.jars.packages`.
> - *`NoClassDefFoundError: com/amazonaws/...` on the s3a write.* Version skew
>   between `hadoop-aws` and the AWS SDK. **Countermeasure:** the pins in
>   `spark/common/session.py` (hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262)
>   match what Spark 3.5.1 is built against. Do not "upgrade" either one alone.
> - *`Connection refused` to `localhost:9000`.* MinIO is down or Project 1 took
>   the port. **Countermeasure:** re-run the pre-flight port check.
> - *`IllegalAccessError` inside Arrow.* Wrong JDK. **Countermeasure:**
>   `echo $JAVA_HOME` must point at 17.
> - *First session takes 4+ minutes.* Normal on first run. Do not kill it.

---

## BLOCK 1.5 (14:15–15:00) — Manifests + landing zone

```bash
python scripts/make_manifest.py --date 2026-07-06
```

Expected:

```
wrote data/manifests/manifest_2026-07-06.json
  bank_2026-07-06.csv             9918 rows  4fee214ae09bffd0...
  internal_2026-07-06.csv        10000 rows  a2a5bcee56727923...
  processor_2026-07-06.csv       10005 rows  766c0b4e9b5b5e32...
```

The SHA256s must equal the ones from Block 1.3b. They are the same bytes; if
they differ, something rewrote the files between steps.

```bash
python scripts/upload_landing.py --date 2026-07-06
```

Expected: four objects under `2026-07-06/` — three CSVs and the manifest. That
key layout is a contract: the Day-5 `S3KeySensor` polls
`recon-landing/{{ ds }}/<source>_{{ ds }}.csv`.

- [ ] Manifest written, hashes match Block 1.3b
- [ ] Four objects in `recon-landing/2026-07-06/`
- [ ] Visible in the MinIO console

> **Pitfall.** The manifest hashes the file **as it sits on disk**. If you ever
> regenerate fixtures without regenerating the manifest, the Day-2 GE checksum
> gate fires and it looks like data corruption. **Countermeasure:** treat
> `seed_generator.py` → `make_manifest.py` → `upload_landing.py` as one
> three-command ritual, never one of the three alone.

---

## BLOCK 1.6 (15:15–16:30) — 🔴 Canonicalize + assert the control line

```bash
python spark/jobs/canonicalize_job.py --date 2026-07-06
```

Expected:

```
canonical/internal: 10000 rows written
canonical/processor: 10005 rows written
canonical/bank: 9918 rows written
canonicalization complete — run scripts/verify_day1.py next
```

Then the assertion that actually matters:

```bash
python scripts/verify_day1.py --date 2026-07-06
```

This prints a per-source per-currency table comparing the canonical Delta
aggregates to `control_line_2026-07-06.json` and exits non-zero on any drift.
It also asserts `amount_minor` is `bigint` (not double), that no key column is
null, and that the answer-key inventory is intact.

Expected last line:

```
DAY 1 VERIFICATION PASSED — canonical tables reproduce the control line to the minor unit
```

- [ ] `canonicalize_job.py` writes three Delta tables
- [ ] `verify_day1.py` exits 0
- [ ] Per-currency rows **and** sums match for all three sources

> **This is the day's actual claim.** Not "the files loaded" — *canonicalisation
> changed representation and not value, and here is the per-currency proof to
> the minor unit.* If a sum drifts, the bug is in a mapper: something cast to
> double, grossed the bank up, or netted the processor's fee. Nothing else in
> the pipeline can move a value yet.
>
> **Countermeasure if `amount_minor` comes back `double`:** you removed the
> explicit `SOURCE_SCHEMAS` schema and let Spark infer. Never `inferSchema` on a
> money column.

---

## BLOCK 1.7 (16:30–17:15) — CI fixture, screenshot, defence drill

```bash
python scripts/seed_generator.py --date 2026-07-06 --rows 1000 --seed 7 \
  --trap-groups 4 --dense-blocks 1 --out data/fixtures/ci_mini/
python scripts/make_manifest.py --date 2026-07-06 \
  --fixtures data/fixtures/ci_mini --out data/fixtures/ci_mini
```

Expected tail:

```
m2m trap groups : 4 (each 2x2 candidates, density 2.0 = greedy path, assert ONE_TO_ONE_STABLE)
dense block     : DENSE-00 density=2.875 edges=23 records=8 greedy=7 pairs / optimal=8 pairs
answer key rows : 3992 (grain: source_system x txn_uid x leg)
ORACLE VERIFIED : greedy loses in all 1 dense block(s); optimal == truth   seed=7
```

The mini fixture is small enough to commit (~520KB) and still contains traps and
a working dense block, so the Day-6 CI matcher gate runs hermetically with no
MinIO. `.gitignore` already excludes the full fixtures and force-includes
`ci_mini/`.

```bash
git check-ignore -v data/fixtures/internal_2026-07-06.csv   # ignored
git status --short data/fixtures/ci_mini/                   # 6 files staged-able
du -sh data/fixtures/ci_mini/
```

- [ ] CI mini fixture generated with its own manifest
- [ ] Full fixtures ignored, `ci_mini/` tracked
- [ ] Screenshot 01 saved in `docs/screenshots/`

---

## BLOCK 1.9 (18:00–18:30) — Review, log, commit

Paste into `docs/daily_log.md`:

```markdown
## Day 1 — Tue 1 Sep 2026 — the answer key is the product

Settled eight contracts before writing any matching logic (docs/day1_decisions.md).
The two that will pay for themselves this week:

- Grain is (source_system, txn_uid, leg). The internal row is in BOTH legs, so it
  has two expected states. At the plan's original one-row-per-(source, txn_uid)
  grain, unioning the two legs double-counts the spine and the conservation
  assertion can never balance — I'd have spent Day 2 debugging a schema bug
  disguised as a lost record.
- Ingestion window: internal is D, bank is D..D+2. TIMING_DIFFERENCE moves the
  settlement, never the transaction. Verified in the fixtures: internal dates
  = [2026-07-06], bank dates = [07-06, 07-07, 07-08].

Fee schedule and FX precision live in conf/recon_config.yml because the generator
must corrupt AMOUNT_MISMATCH rows beyond tolerance, and tolerance is derived from
the fee schedule. If the generator had its own copy of those numbers, Day 3's
derived tolerance would silently disagree with Day 1's corruption and the answer
key would be wrong while every check stayed green.

Bank amounts stay net. Grossing them up with the processor's own fee column would
be reconciling the data to itself. The gross/net gap is absorbed by a tolerance
derived from versioned reference data, which a point-in-time rerun can reproduce;
a fixup inside a mapper cannot.

Found my own m:n trap was unfalsifiable. The plan wanted the answer key to name
"the intended pairing" for two identical transactions with two identical
settlements — but the data carries no signal that distinguishes them, so the only
way to pass would be to leak the answer into the matcher. Changed the assertion to
the property the system actually guarantees: one-to-one, no fan-out, both matched,
identical assignment across runs.

Dense blocks: 6 "band" rows to push candidate density to 2.875, plus a 2-row
"chain" where a wrong edge (diff 100) outscores both true edges (166, 174) while
the second transaction's own settlement sits at 440, beyond the 302 tolerance, so
greedy strands it. Optimal wins because 1/167 + 1/175 > 1/101. Measured: greedy 7
pairs, optimal 8, optimal == truth. The generator asserts all three properties with
scipy before writing a single CSV — a demo is only a demo if greedy loses.

Two venvs (spark / airflow, Day 5). GE held back to Day 2 in its own requirements
file: biggest dependency tree in the project, must not be able to block oracle day.

Containers prefixed recon-; Project 1 already owns the name `minio` and ports
9000/9001/5432. Airflow Postgres on 5433. One stack at a time on 16GB — a
documented constraint, not an accident.
```

```bash
git add -A
git commit -m "day1: oracle + canonical tables — generator deterministic, dense blocks provably defeat greedy, canonical tables reproduce control line to the minor unit"
```

- [ ] Daily log written
- [ ] Committed

---

## 3. DAY 1 EXIT CRITERIA

Every box, or Day 2 starts on sand.

- [ ] Two runs at `--seed 42` produce byte-identical CSVs, answer key and control line; hashes match the reference set
- [ ] Class distribution within tolerance of the spec: 5.10 / 0.89 / 0.82 / 0.94 / 3.04 %
- [ ] 20 trap groups and 3 dense blocks present; oracle self-verification passes (density 2.875, greedy 7 / optimal 8, optimal == truth)
- [ ] Every `AMOUNT_MISMATCH` is beyond tolerance; every true bank pair is inside it
- [ ] Answer key grained by `(source_system, txn_uid, leg)` with no duplicate keys, 39,923 rows
- [ ] Landing zone holds 3 CSVs + manifest under `2026-07-06/`; manifest hashes equal the on-disk hashes
- [ ] Three canonical Delta tables on `recon-lake`, `amount_minor` typed `bigint`, no nulls in key columns
- [ ] `verify_day1.py` exits 0 — per-currency rows and sums match the control line for all three sources
- [ ] CI mini fixture committed; full fixtures gitignored
- [ ] Screenshot 01 captured
- [ ] `docs/day1_decisions.md` + daily log written; committed

---

## 4. CARRY FORWARD — what Day 1 tells you about Days 2–4

Written down now, while it is cheap.

**Day 2 — the block key is the merchant.** `txn_ref` is merchant-prefixed and
`block_prefix_len_default` is 4, so `block_key = currency | week | merchant`.
That is intentional (it makes Day 4's whale demo physically real) but it means
the 4-char prefix contributes zero ref-level selectivity. At 10k rows blocks
hold roughly 50–100 records, which is fine. Say this out loud in the README
rather than letting an interviewer discover it.

**Day 2 — resolve cross-block conflicts.** Dual-bucket blocking puts a record in
up to two blocks, and the resolver runs *per block* under `applyInPandas`. Two
blocks can therefore assign the same `txn_uid` to different counterparts and the
one-to-one guarantee is only local. You need a global deterministic reduce after
resolution — score desc, then both refs, mergesort — or Day 3's precision table
will show phantom failures concentrated on exactly the timing-difference rows.
The plan mentions deduping after the *exact* pass and never after resolution.

**Day 3 — measure candidate density before trusting the 2.0 threshold.** The
bank-leg tolerance is `max_fee + fx + ε`, which is wide relative to the *actual*
fee on a small transaction. Print the density distribution across all blocks
before asserting "Hungarian activates on exactly the 3 dense blocks." If normal
blocks come out above 2.0, the fix is a fee-adjusted residual as the scoring
input (`|bank_amount − (gross − expected_fee(gross))|`) with the tolerance
formula unchanged — not a threshold tweak.

**Day 4 — cap the whale date at ~30k rows.** With `block_key` = merchant, a
whale at 40% of 60k rows lands ~6,000 records in a single block, and the bank leg
has no exact-match exit, so candidate generation there is a within-block
cartesian of ~36M pairs handed to one pandas frame. That is an OOM, not a
straggler histogram. At 20k rows the largest cell measured 2,067 records, which
trips the >5000 WARN only if you go higher — 30k is the sweet spot: WARN fires,
laptop survives. `seed_generator.py` already accepts `--whale M007
--whale-share 0.40` and produces a verified 0.409 share.

---

*Day 1 of 6 — Multi-Source Settlement Reconciliation Engine — Aditya Gurematti, UT Dallas*
