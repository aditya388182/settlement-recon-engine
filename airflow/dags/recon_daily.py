from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Absolute paths, resolved once. AIRFLOW_HOME/dags is a symlink to the repo, so
# derive the repo root from this file rather than from the working directory.
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(DAGS_DIR)
SPARK_PY = os.path.join(REPO, ".venv-spark", "bin", "python")
STAGING = "/tmp/recon_staging/{{ ds }}"

SOURCES = ("internal", "processor", "bank")

DEFAULT_ARGS = {
    "owner": "recon",
    "retries": 0,                 # a failed gate must stay failed and visible
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="recon_daily",
    description="Multi-source settlement reconciliation for one delivery",
    start_date=datetime(2026, 7, 6),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reconciliation", "settlement"],
) as dag:

    sensors = {
        src: S3KeySensor(
            task_id=f"wait_{src}",
            bucket_name="recon-landing",
            bucket_key="{{ ds }}/" + src + "_{{ ds }}.csv",
            aws_conn_id="minio_s3",
            poke_interval=30,
            timeout=60 * 60 * 6,
            mode="reschedule",
        )
        for src in SOURCES
    }

    stage = BashOperator(
        task_id="stage_delivery",
        bash_command=(f"{SPARK_PY} {REPO}/scripts/fetch_landing.py "
                      f"--date {{{{ ds }}}} --dest {STAGING}"),
    )

    # GE gates ingestion; it is NEVER the correctness mechanism. Control totals
    # and the answer key are. A non-zero exit here stops the run before the
    # engine ever reconciles against garbage.
    gates = {
        src: BashOperator(
            task_id=f"ge_{src}",
            bash_command=(f"{SPARK_PY} {REPO}/great_expectations/run_checkpoint.py "
                          f"--source {src} --date {{{{ ds }}}} "
                          f"--fixtures {STAGING} --manifests {STAGING}"),
        )
        for src in SOURCES
    }

    canonicalize = BashOperator(
        task_id="canonicalize",
        bash_command=(f"{SPARK_PY} {REPO}/spark/jobs/canonicalize_job.py "
                      f"--date {{{{ ds }}}}"),
    )

    run_recon = BashOperator(
        task_id="run_recon",
        bash_command=(f"{SPARK_PY} {REPO}/spark/jobs/recon_job.py "
                      f"--date {{{{ ds }}}}"),
    )

    publish_metrics = BashOperator(
        task_id="publish_metrics",
        bash_command=(f"{SPARK_PY} {REPO}/scripts/publish_metrics.py "
                      f"--date {{{{ ds }}}}"),
        # failure paths too, or the Sev-1 alert is blind exactly when it matters.
        trigger_rule="all_done",
    )

    for src in SOURCES:
        sensors[src] >> stage
    for src in SOURCES:
        stage >> gates[src] >> canonicalize

    canonicalize >> run_recon >> publish_metrics
