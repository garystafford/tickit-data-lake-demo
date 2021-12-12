import os
from datetime import timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

CRAWLERS = ["tickit_mssql", "tickit_mysql", "tickit_postgresql"]

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Run AWS Glue Crawlers to catalog data from RDS data sources",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["data lake demo", "source"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command="""aws glue get-tables --database-name tickit_demo \
                          --query 'TableList[].Name' --expression "source_*"  \
                          --output table""",
    )

    for crawler in CRAWLERS:
        crawlers_run = AwsGlueCrawlerOperator(
            task_id=f"run_{crawler}_crawler", config={"Name": crawler}
        )

        chain(
            begin,
            crawlers_run,
            list_glue_tables,
            end,
        )
