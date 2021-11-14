import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['{{ var.value.fail_email }}'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
        dag_id=DAG_ID,
        description='Run AWS Glue ETL Jobs: source-to-raw',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=15),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['data lake demo', 'glue job', 'raw', 'bronze']
) as dag:
    job_start_event_raw = AwsGlueJobOperator(
        task_id='job_start_event_raw',
        job_name='tickit_public_event_raw'
    )

    job_start_users_raw = AwsGlueJobOperator(
        task_id='job_start_users_raw',
        job_name='tickit_public_users_raw'
    )

    job_start_category_raw = AwsGlueJobOperator(
        task_id='job_start_category_raw',
        job_name='tickit_public_category_raw'
    )

    job_start_date_raw = AwsGlueJobOperator(
        task_id='job_start_date_raw',
        job_name='tickit_public_date_raw'
    )

    job_start_listing_raw = AwsGlueJobOperator(
        task_id='job_start_listing_raw',
        job_name='tickit_public_listing_raw'
    )

    job_start_sales_raw = AwsGlueJobOperator(
        task_id='job_start_sales_raw',
        job_name='tickit_public_sales_raw'
    )

    job_start_venue_raw = AwsGlueJobOperator(
        task_id='job_start_venue_raw',
        job_name='tickit_public_venue_raw'
    )

    list_glue_tables = BashOperator(
        task_id='list_glue_tables',
        bash_command="""aws glue get-tables --database-name tickit_demo \
                          --query 'TableList[].Name' --expression "raw_*"  \
                          --output table"""
    )

[job_start_event_raw, job_start_users_raw, job_start_category_raw,
 job_start_date_raw, job_start_listing_raw, job_start_sales_raw,
 job_start_venue_raw] >> list_glue_tables
