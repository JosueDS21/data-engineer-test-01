"""
Airflow DAG: run the Airbnb ETL pipeline on a schedule.
Requires Airflow + project code (or install as package). Configure connection and variables for DB.
"""
from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.getenv("AIRFLOW__DATA_ENGINEER_TEST__PROJECT_ROOT", "/path/to/data-engineer-test-01")


def run_etl():
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    os.environ["PROJECT_ROOT"] = PROJECT_ROOT
    from src.pipeline.orchestrator import run_pipeline
    run_pipeline()


with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "airbnb"],
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
