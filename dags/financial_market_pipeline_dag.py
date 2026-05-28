from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "samuel",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="financial_market_pipeline",

    default_args=default_args,

    description="Financial market medallion pipeline",

    start_date=datetime(2026, 1, 1),

    schedule="@daily",

    catchup=False,

    tags=["finance", "medallion", "duckdb"],
) as dag:

    ingest_task = BashOperator(
        task_id="ingest_market_data",

        bash_command="python /opt/airflow/src/ingest.py",
    )

    silver_task = BashOperator(
        task_id="transform_silver_layer",

        bash_command="python /opt/airflow/src/transform_silver.py",
    )

    gold_task = BashOperator(
        task_id="transform_gold_layer",

        bash_command="python /opt/airflow/src/transform_gold.py",
    )

    ingest_task >> silver_task >> gold_task