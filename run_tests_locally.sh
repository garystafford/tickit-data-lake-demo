#!/bin/bash

# Purpose: Run Airflow DAG tests locally before committing and pushing
# Author: Gary A. Stafford
# Modified: 2022-01-16
# Run this command first:
# python3 -m pip install --user -U -r requirements_local_tests.txt

bold=$(tput bold)
normal=$(tput sgr0)

# this doesn't make sense for pre-push - move to pre-commit
#echo "\nFormatting DAGs using Black..."
#black dags/

echo "\n⌛ Starting Flake8 test..."
python3 -m flake8 --ignore E501 dags --benchmark || exit 1

echo "\n⌛ Starting Black test..."
python3 -m pytest --cache-clear
python3 -m pytest dags/ --black -v || exit 1

echo "\n⌛ Starting Pytest tests..."
pushd tests || exit 1
python3 -m pytest tests.py -v || exit 1
popd || exit 1

echo "\n⌛ Starting SQLFluff tests..."
pushd dags || exit 1
python3 -m sqlfluff lint \
  --dialect redshift \
  --ignore parsing,templating \
  --format yaml \
  sql_redshift/ || exit 1
python3 -m sqlfluff lint \
  --dialect hive \
  --ignore parsing,templating \
  --format yaml \
  sql_data_lake/  || exit 1
popd || exit 1

echo "\n⌛ Starting JSON validation tests..."
python3 -m json.tool airflow_variables/variables.json

echo "${bold}\nAll tests completed successfully! 🥳\n${normal}"