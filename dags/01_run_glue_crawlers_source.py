import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
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
        description='Run AWS Glue Crawlers',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=30),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['data lake demo', 'glue crawler', 'source']
) as dag:
    crawler_mssql_start = AwsGlueCrawlerOperator(
        task_id='mssql_crawler_start',
        config={'Name': 'tickit_mssql'}
    )

    crawler_mysql_start = AwsGlueCrawlerOperator(
        task_id='mysql_crawler_start',
        config={'Name': 'tickit_mysql'}
    )

    crawler_postgresql_start = AwsGlueCrawlerOperator(
        task_id='postgresql_crawler_start',
        config={'Name': 'tickit_postgresql'}
    )

    list_glue_tables = BashOperator(
        task_id='list_glue_tables',
        bash_command="""aws glue get-tables --database-name tickit_demo \
                          --query 'TableList[].Name' --expression "source_*"  \
                          --output table"""
    )

[crawler_mssql_start, crawler_mysql_start, crawler_postgresql_start] >> list_glue_tables
