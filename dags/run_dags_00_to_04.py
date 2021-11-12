import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        description='Run all Data Lake demonstration DAGs',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=30),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['data lake demo']
) as dag:
    trigger_dag_00 = TriggerDagRunOperator(
        task_id="trigger_dag_00",
        trigger_dag_id="00_clean_and_prep_demo",
        wait_for_completion=True
    )

    trigger_dag_01 = TriggerDagRunOperator(
        task_id="trigger_dag_01",
        trigger_dag_id="01_run_glue_crawlers_source",
        wait_for_completion=True
    )

    trigger_dag_02 = TriggerDagRunOperator(
        task_id="trigger_dag_02",
        trigger_dag_id="02_run_glue_jobs_raw",
        wait_for_completion=True
    )

    trigger_dag_03 = TriggerDagRunOperator(
        task_id="trigger_dag_03",
        trigger_dag_id="03_run_glue_jobs_refined",
        wait_for_completion=True
    )
    trigger_dag_04 = TriggerDagRunOperator(
        task_id="trigger_dag_04",
        trigger_dag_id="04_submit_athena_queries_agg",
        wait_for_completion=True
    )

trigger_dag_00 >> trigger_dag_01 >> trigger_dag_02 >> trigger_dag_03 >> trigger_dag_04
