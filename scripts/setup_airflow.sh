#!/usr/bin/env bash
# setup_airflow.sh — Airflow 2.9 in its OWN virtualenv.
#
# Two venvs, on purpose. PySpark 3.5 and Airflow 2.9 have incompatible pins on
# pendulum, sqlalchemy and protobuf; one venv is a day lost to the pip resolver.
# Airflow NEVER imports pyspark — it shells out to the engine with absolute
# paths. That is a process boundary, not a Python import, and it is also the
# honest production shape: the scheduler is not the runtime.
#
# Airflow runs on the host rather than in Docker: a 16GB laptop does not want the
# Airflow image sitting next to Spark. Same call as "Spark on host" in Project 1.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m venv .venv-airflow
# shellcheck disable=SC1091
source .venv-airflow/bin/activate
pip install --upgrade pip

# ALWAYS install with the constraints file. An unpinned Airflow install is a
# tarpit: the resolver will happily spend twenty minutes and hand you a broken
# environment.
pip install "apache-airflow==${AIRFLOW_VERSION}" \
            "apache-airflow-providers-amazon" \
            --constraint "${CONSTRAINTS}"

export AIRFLOW_HOME="$REPO_ROOT/airflow_home"
mkdir -p "$AIRFLOW_HOME"

# LocalExecutor + the compose Postgres. SQLite plus SequentialExecutor cannot run
# tasks in parallel, so the three sensors would serialise and the DAG would look
# broken for reasons that have nothing to do with the data.
cat > "$AIRFLOW_HOME/airflow.cfg.overrides" <<'CFG'
[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5433/airflow
[core]
executor = LocalExecutor
load_examples = False
CFG

airflow config list >/dev/null 2>&1 || true
python - <<'PY'
import configparser, os
home = os.environ["AIRFLOW_HOME"]
cfg_path = os.path.join(home, "airflow.cfg")
if not os.path.exists(cfg_path):
    raise SystemExit("airflow.cfg not created yet; run `airflow version` once, then re-run")
cfg = configparser.ConfigParser()
cfg.read(cfg_path)
cfg["database"]["sql_alchemy_conn"] = \
    "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"
cfg["core"]["executor"] = "LocalExecutor"
cfg["core"]["load_examples"] = "False"
with open(cfg_path, "w") as fh:
    cfg.write(fh)
print("airflow.cfg: LocalExecutor + compose Postgres on 5433")
PY

airflow db migrate

# The endpoint_url extra is what points the S3 hooks at MinIO. Forget and the
# sensors dial real AWS hang there politely six hours.
airflow connections delete minio_s3 >/dev/null 2>&1 || true
airflow connections add minio_s3 --conn-type aws \
  --conn-extra '{"endpoint_url": "http://localhost:9000",
                 "aws_access_key_id": "minioadmin",
                 "aws_secret_access_key": "minioadmin",
                 "region_name": "us-east-1"}'

ln -sfn "$REPO_ROOT/airflow/dags" "$AIRFLOW_HOME/dags"

cat <<EOF

Airflow is set up. AIRFLOW_HOME=$AIRFLOW_HOME

Run these in two terminals, both with the venv active and AIRFLOW_HOME exported:

  source .venv-airflow/bin/activate && export AIRFLOW_HOME=$AIRFLOW_HOME
  airflow scheduler

  source .venv-airflow/bin/activate && export AIRFLOW_HOME=$AIRFLOW_HOME
  airflow webserver -p 8080

EOF
