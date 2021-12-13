# Purpose: Pytest test for Data Lake and Redshift
#          Airflow Demonstration project DAGs
# Author: Gary A. Stafford
# Modified: 2021-12-10

import os
import sys

import pytest
from airflow.models import DagBag

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../dags/utilities"))

# Airflow variables called from DAGs under test are stubbed out
os.environ["AIRFLOW_VAR_DATA_LAKE_BUCKET"] = "test_bucket"
os.environ["AIRFLOW_VAR_ATHENA_QUERY_RESULTS"] = "SELECT 1;"
os.environ["AIRFLOW_VAR_SNS_TOPIC"] = "test_topic"
os.environ["AIRFLOW_VAR_REDSHIFT_UNLOAD_IAM_ROLE"] = "test_role_1"
os.environ["AIRFLOW_VAR_GLUE_CRAWLER_IAM_ROLE"] = "test_role_2"


@pytest.fixture(params=["../dags/"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors


def test_requires_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags


def test_requires_specific_tag(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        try:
            assert dag.tags.index("data lake demo") >= 0
        except ValueError:
            assert dag.tags.index("redshift demo") >= 0


def test_desc_len_greater_than_fifteen(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.description) > 15


def test_owner_len_greater_than_five(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.owner) > 5


def test_owner_not_airflow(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert str.lower(dag.owner) != "airflow"


def test_no_emails_on_retry(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert not dag.default_args["email_on_retry"]


def test_no_emails_on_failure(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert not dag.default_args["email_on_failure"]


def test_three_or_less_retries(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args["retries"] <= 3


def test_dag_id_contains_prefix(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert str.lower(dag_id).find("__") != -1


def test_dag_id_requires_specific_prefix(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert str.lower(dag_id).startswith("data_lake__") \
               or str.lower(dag_id).startswith("redshift_demo__")
