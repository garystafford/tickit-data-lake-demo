import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

SNS_TOPIC = Variable.get("sns_topic")

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}


def _sns_success_notification(context):
    task_instances = context.get("dag_run").get_task_instances()

    sns_publish = SnsPublishOperator(
        task_id="publish_sns_message",
        aws_conn_id="aws_default",
        target_arn=SNS_TOPIC,
        message=f"These task instances succeeded: {task_instances}.",
        subject=f"{DAG_ID} Completed Successfully",
    )

    return sns_publish.execute(context=context)


def _sns_failure_notification(context):
    task_instances = context.get("dag_run").get_task_instances()

    sns_publish = SnsPublishOperator(
        task_id="publish_sns_message",
        aws_conn_id="aws_default",
        target_arn=SNS_TOPIC,
        message=f"These task instances failed: {task_instances}.",
        subject=f"{DAG_ID} Failed!",
    )

    return sns_publish.execute(context=context)


def _slack_failure_notification(context):
    slack_msg = f"""
            :red_circle: DAG Failed.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            """
    failed_alert = SlackWebhookOperator(
        task_id="slack_notification", http_conn_id="slack_webhook", message=slack_msg
    )

    return failed_alert.execute(context=context)


def _slack_success_notification(context):
    slack_msg = f"""
            :large_green_circle: DAG Succeeded.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            """
    success_alert = SlackWebhookOperator(
        task_id="slack_notification", http_conn_id="slack_webhook", message=slack_msg
    )

    return success_alert.execute(context=context)


with DAG(
    dag_id=DAG_ID,
    description="Run all Redshift demonstration DAGs",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=45),
    start_date=days_ago(1),
    schedule_interval=None,
    on_failure_callback=_slack_failure_notification,
    on_success_callback=_slack_success_notification,
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
