import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

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
        description="Prepare for data lake demonstration",
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=5),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=["data lake demo"]
) as dag:
    delete_demo_s3_objects = BashOperator(
        task_id="delete_demo_s3_objects",
        bash_command='aws s3 rm "s3://{{ var.value.data_lake_bucket }}/tickit/" --recursive'
    )

    list_demo_s3_objects = BashOperator(
        task_id="list_demo_s3_objects",
        bash_command='aws s3api list-objects-v2 --bucket {{ var.value.data_lake_bucket }} --prefix tickit/'
    )

    delete_demo_catalog = BashOperator(
        task_id="delete_demo_catalog",
        bash_command='aws glue delete-database --name tickit_demo || echo "Database tickit_demo not found."'
    )

    create_demo_catalog = BashOperator(
        task_id="create_demo_catalog",
        bash_command="""aws glue create-database --database-input \
            '{"Name": "tickit_demo", "Description": "Track sales activity for the fictional TICKIT web site"}'"""
    )

delete_demo_s3_objects >> list_demo_s3_objects
delete_demo_catalog >> create_demo_catalog
