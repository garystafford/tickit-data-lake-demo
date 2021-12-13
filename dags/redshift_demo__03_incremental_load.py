import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.sql import SQLThresholdCheckOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
SCHEMA = "tickit_demo"
TABLES = ["listing", "sales"]
BEGIN_DATE = "2008-02-01"
END_DATE = "2008-02-28"

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "redshift_conn_id": "amazon_redshift_dev",
    "postgres_conn_id": "amazon_redshift_dev",
}

with DAG(
    dag_id=DAG_ID,
    description="Load incremental sales and listing data into Amazon Redshift",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["redshift demo"],
) as dag:
    begin = DummyOperator(task_id="begin")

    begin_qc = DummyOperator(task_id="begin_qc")

    end = DummyOperator(task_id="end")

    check_new_listing_count = SQLThresholdCheckOperator(
        task_id="check_max_date_listing",
        conn_id=DEFAULT_ARGS["redshift_conn_id"],
        sql=f"SELECT COUNT(*) FROM {SCHEMA}.listing WHERE listtime >= {BEGIN_DATE}",
        min_threshold=1,
        max_threshold=100000,
    )

    check_new_sales_count = SQLThresholdCheckOperator(
        task_id="check_max_date_sales",
        conn_id=DEFAULT_ARGS["redshift_conn_id"],
        sql=f"SELECT COUNT(*) FROM {SCHEMA}.sales WHERE saletime >= {BEGIN_DATE}",
        min_threshold=1,
        max_threshold=100000,
    )

    for table in TABLES:
        create_staging_tables = PostgresOperator(
            task_id=f"create_table_{table}_staging",
            sql=f"sql_redshift/create_table_{table}_staging.sql",
        )

        truncate_staging_tables = PostgresOperator(
            task_id=f"truncate_table_{table}_staging",
            sql=f"TRUNCATE TABLE {SCHEMA}.{table}_staging;",
        )

        s3_to_staging_tables = S3ToRedshiftOperator(
            task_id=f"{table}_to_staging",
            s3_bucket=S3_BUCKET,
            s3_key=f"redshift/data/{table}.gz",
            schema=SCHEMA,
            table=f"{table}_staging",
            copy_options=["gzip", "delimiter '|'"],
        )

        merge_staging_data = PostgresOperator(
            task_id=f"merge_{table}",
            sql=f"sql_redshift/merge_{table}.sql",
            params={"begin_date": BEGIN_DATE, "end_date": END_DATE},
        )

        drop_staging_tables = PostgresOperator(
            task_id=f"drop_{table}_staging",
            sql=f"DROP TABLE IF EXISTS {SCHEMA}.{table}_staging;",
        )

        chain(
            begin,
            create_staging_tables,
            truncate_staging_tables,
            s3_to_staging_tables,
            merge_staging_data,
            drop_staging_tables,
            begin_qc,
            (check_new_listing_count, check_new_sales_count),
            end,
        )
