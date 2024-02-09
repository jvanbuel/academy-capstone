import datetime

from airflow import DAG
from airflow.utils import dates
from datetime import timedelta

# from airflow.providers.amazon.aws.operators.batch import BatchOperator
from conveyor.operators import ConveyorContainerOperatorV2


default_args = {
    "owner": "Conveyor",
    "depends_on_past": False,
    "start_date": dates.days_ago(2),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="jan-capstone-winter-school-2024-dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# BatchOperator(
#     dag=dag,
#     task_id="snowflake_ingest",
#     job_definition="Jan-winter-school-capstone",
#     job_queue="academy-capstone-winter-2024-job-queue",
#     job_name="snowflake_ingest_jan",
#     overrides={},
# )


with DAG(
    "capstone_jan",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
) as dag:
    sample_task = ConveyorContainerOperatorV2(
        dag=dag,
        task_id="ingest_to_snowflake",
        instance_type="mx.small",
        aws_role="arn:aws:iam::130966031144:role/capstone_conveyor",
    )
