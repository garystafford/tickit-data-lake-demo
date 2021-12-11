#!/bin/sh

# Purpose: Run Airflow DAG tests locally before committing and pushing
# Author: Gary A. Stafford
# Modified: 2021-12-11
# Run this command first: python3 -m pip install --user -U -r requirements_local_tests.txt

echo "Formatting DAGs using black..."
black ./dags/

echo "Starting flake8 test..."
flake8 --ignore E501 ./dags/

echo "Starting black test..."
python3 -m pytest ./dags/ --black

echo "Starting pytest tests..."
python3 -m pytest tests/ --no-header --doctest-modules --cov=dags -v

echo "All test completed. Check the results."