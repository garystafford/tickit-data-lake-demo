#!/bin/sh

# Purpose: Run Airflow DAG tests locally before committing and pushing
# Author: Gary A. Stafford
# Modified: 2021-12-11
# Run this command first: python3 -m pip install --user -U -r requirements_local_tests.txt

echo "\nFormatting DAGs using black..."
black dags/

echo "\nStarting flake8 test..."
flake8 --ignore E501 dags --benchmark

echo "\nStarting black test..."
python3 -m pytest dags/ --black -v

echo "\nStarting pytest tests..."
cd tests || exit
python3 -m pytest tests.py -v

echo "\nAll test completed. Check the results."