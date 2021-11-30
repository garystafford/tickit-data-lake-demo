import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

CRAWLERS = ["tickit_mssql", "tickit_mysql", "tickit_postgresql"]

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
        dag_id=DAG_ID,
        description="Run AWS Glue Crawlers",
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=15),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=["data lake demo", "glue crawler", "source"]
) as dag:
    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command="""aws glue get-tables --database-name tickit_demo \
                          --query 'TableList[].Name' --expression "source_*"  \
                          --output table"""
    )

    for crawler in CRAWLERS:
        crawler_run = AwsGlueCrawlerOperator(
            task_id=f"{crawler}_crawler_run",
            config={"Name": crawler}
        )

crawler_run >> list_glue_tables
