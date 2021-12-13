# Alternative version of DAG 06 with notification logic stored in external module,
# which sends notifications to Amazon SNS or Slack.

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from utilities.notifications import sns_success_notification, sns_failure_notification

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Run all Redshift demonstration DAGs",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=45),
    start_date=days_ago(1),
    schedule_interval=None,
    on_failure_callback=sns_failure_notification,
    on_success_callback=sns_success_notification,
    tags=["redshift demo"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    trigger_dag_01 = TriggerDagRunOperator(
        task_id="trigger_dag_01",
        trigger_dag_id="redshift_demo__01_create_tables",
        wait_for_completion=True,
    )

    trigger_dag_02 = TriggerDagRunOperator(
        task_id="trigger_dag_02",
        trigger_dag_id="redshift_demo__02_initial_load",
        wait_for_completion=True,
    )

    trigger_dag_03 = TriggerDagRunOperator(
        task_id="trigger_dag_03",
        trigger_dag_id="redshift_demo__03_incremental_load",
        wait_for_completion=True,
    )

    trigger_dag_04 = TriggerDagRunOperator(
        task_id="trigger_dag_04",
        trigger_dag_id="redshift_demo__04_unload_data",
        wait_for_completion=True,
    )
    trigger_dag_05 = TriggerDagRunOperator(
        task_id="trigger_dag_05",
        trigger_dag_id="redshift_demo__05_catalog_and_query",
        wait_for_completion=True,
    )

    chain(
        begin,
        trigger_dag_01,
        trigger_dag_02,
        trigger_dag_03,
        trigger_dag_04,
        trigger_dag_05,
        end,
    )
