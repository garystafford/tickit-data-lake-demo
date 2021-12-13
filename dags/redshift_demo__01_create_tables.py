import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
SCHEMA = "tickit_demo"
TABLES = ["users", "venue", "category", "date", "event", "listing", "sales"]

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "postgres_conn_id": "amazon_redshift_dev",
}

with DAG(
    dag_id=DAG_ID,
    description="Create tickit_demo schema tables in Amazon Redshift",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["redshift demo"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    create_tables = PostgresOperator(
        task_id="create_tables", sql="sql_redshift/create_tables.sql"
    )

    for table in TABLES:
        drop_tables = PostgresOperator(
            task_id=f"drop_table_{table}", sql=f"DROP TABLE IF EXISTS {SCHEMA}.{table};"
        )

        chain(begin, drop_tables, create_tables, end)
