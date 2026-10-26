from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="e2e_neon_to_warehouse",
    description="Neon ingestion, warehouse loading and dbt transformations",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
    },
    tags=["e2e", "neon", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_source_to_bronze",
        bash_command="python /opt/project/src/ingest.py",
        cwd="/opt/project",
    )

    load_warehouse = BashOperator(
        task_id="load_bronze_to_warehouse",
        bash_command="python /opt/project/src/load_warehouse.py",
        cwd="/opt/project",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run --profiles-dir /opt/airflow/config/dbt"
        ),
        cwd="/opt/project",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test --profiles-dir /opt/airflow/config/dbt"
        ),
        cwd="/opt/project",
    )

    ingest >> load_warehouse >> dbt_run >> dbt_test
