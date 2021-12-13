import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
S3_UNLOAD_PATH = f"s3://{S3_BUCKET}/redshift/sales/"
GLUE_CRAWLER_IAM_ROLE = Variable.get("glue_crawler_iam_role")
GLUE_DATABASE = "tickit_redshift_demo"

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "postgres_conn_id": "amazon_redshift_dev",
}

with DAG(
    dag_id=DAG_ID,
    description="Catalog unloaded data with Glue and query with Athena",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["redshift demo"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    delete_glue_catalog = BashOperator(
        task_id="delete_demo_catalog",
        bash_command=f'aws glue delete-database --name {GLUE_DATABASE} || echo "Database tickit_redshift_demo not found."',
    )

    create_glue_catalog = BashOperator(
        task_id="create_demo_catalog",
        bash_command="""aws glue create-database --database-input \
            '{"Name": "tickit_redshift_demo", "Description": "TICKIT sales data unloaded from Redshift"}'""",
    )

    crawl_unloaded_data = AwsGlueCrawlerOperator(
        task_id="crawl_unloaded_data",
        config={
            "Name": "tickit-sales-unloaded",
            "Role": GLUE_CRAWLER_IAM_ROLE,
            "DatabaseName": GLUE_DATABASE,
            "Description": "Crawl TICKIT sales data unloaded from Redshift",
            "Targets": {"S3Targets": [{"Path": S3_UNLOAD_PATH}]},
        },
    )

    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command=f"""aws glue get-tables --database-name {GLUE_DATABASE} \
                          --query 'TableList[].Name' \
                          --output table""",
    )

    athena_query_glue = AWSAthenaOperator(
        task_id="athena_query_glue",
        query="""SELECT *
            FROM tickit_redshift_demo.sales
            WHERE catgroup = 'Shows' AND catname = 'Opera'
            ORDER BY saletime
            LIMIT 10;""",
        output_location="s3://{{ var.value.athena_query_results }}/",
        database=GLUE_DATABASE,
    )

    chain(
        begin,
        delete_glue_catalog,
        create_glue_catalog,
        crawl_unloaded_data,
        list_glue_tables,
        athena_query_glue,
        end,
    )
