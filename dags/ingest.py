import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator

dag = DAG(
    dag_id="jan-capstone-winter-school-2024-dag",
    default_view="graph",
    schedule_interval=None,
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
)

BatchOperator(
    dag=dag,
    task_id="snowflake_ingest",
    job_definition="Jan-winter-school-capstone",
    job_queue="academy-capstone-winter-2024-job-queue",
    job_name="snowflake_ingest_jan",
    overrides={},
)
