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
        description='Run AWS Glue ETL Jobs: raw-to-refined',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=15),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['data lake demo', 'glue job', 'refined', 'silver']
) as dag:
    job_start_event_refined = AwsGlueJobOperator(
        task_id='job_start_event_refined',
        job_name='tickit_public_event_refine'
    )

    job_start_users_refined = AwsGlueJobOperator(
        task_id='job_start_users_refined',
        job_name='tickit_public_users_refine'
    )

    job_start_category_refined = AwsGlueJobOperator(
        task_id='job_start_category_refined',
        job_name='tickit_public_category_refine'
    )

    job_start_date_refined = AwsGlueJobOperator(
        task_id='job_start_date_refined',
        job_name='tickit_public_date_refine'
    )

    job_start_listing_refined = AwsGlueJobOperator(
        task_id='job_start_listing_refined',
        job_name='tickit_public_listing_refine'
    )

    job_start_sales_refined = AwsGlueJobOperator(
        task_id='job_start_sales_refined',
        job_name='tickit_public_sales_refine')

    job_start_venue_refined = AwsGlueJobOperator(
        task_id='job_start_venue_refined',
        job_name='tickit_public_venue_refine'
    )

    list_glue_tables = BashOperator(
        task_id='list_glue_tables',
        bash_command="""aws glue get-tables --database-name tickit_demo \
                          --query 'TableList[].Name' --expression "refined_*"  \
                          --output table"""
    )

[job_start_event_refined, job_start_users_refined, job_start_category_refined,
 job_start_date_refined, job_start_listing_refined, job_start_sales_refined,
 job_start_venue_refined] >> list_glue_tables
