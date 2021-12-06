# Notification logic for DAGs

import os

from airflow.models import Variable
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")
SNS_TOPIC = Variable.get("sns_topic")


def sns_success_notification(context):
    task_instances = context.get("dag_run").get_task_instances()

    sns_publish = SnsPublishOperator(
        task_id="publish_sns_message",
        aws_conn_id="aws_default",
        target_arn=SNS_TOPIC,
        message=f"These task instances succeeded: {task_instances}.",
        subject=f"{context.get('dag')} Completed Successfully",
    )

    return sns_publish.execute(context=context)


def sns_failure_notification(context):
    task_instances = context.get("dag_run").get_task_instances()

    sns_publish = SnsPublishOperator(
        task_id="publish_sns_message",
        aws_conn_id="aws_default",
        target_arn=SNS_TOPIC,
        message=f"These task instances failed: {task_instances}.",
        subject=f"{context.get('dag')} Failed!",
    )

    return sns_publish.execute(context=context)


def slack_failure_notification(context):
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


def slack_success_notification(context):
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
