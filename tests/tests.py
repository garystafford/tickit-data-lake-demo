import os
import sys

import pytest
from airflow.models import DagBag

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags/utilities"))

os.environ["AIRFLOW_VAR_DATA_LAKE_BUCKET"] = "my_bucket"
os.environ["AIRFLOW_VAR_ATHENA_QUERY_RESULTS"] = "SELECT 1;"
os.environ["AIRFLOW_VAR_SNS_TOPIC"] = "my_topic"
os.environ["AIRFLOW_VAR_REDSHIFT_UNLOAD_IAM_ROLE"] = "my_role"
os.environ["AIRFLOW_VAR_GLUE_CRAWLER_IAM_ROLE"] = "my_role"


@pytest.fixture(params=["."])
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


def test_requires_specific_prefix(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert str.lower(dag_id).startswith("data_lake__") \
               or str.lower(dag_id).startswith("redshift_demo__")
