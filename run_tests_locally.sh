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

echo "\nStarting Flake8 test..."
flake8 --ignore E501 dags --benchmark || exit 1

echo "\nStarting Black test..."
python3 -m pytest --cache-clear
python3 -m pytest dags/ --black -v || exit 1

echo "\nStarting Pytest tests..."
pushd tests || exit 1
python3 -m pytest tests.py -v || exit 1
popd || exit 1

echo "\nStarting SQLFluff tests..."
pushd dags || exit 1
sqlfluff lint --dialect redshift \
  --ignore parsing,templating \
  --format yaml \
  sql_redshift/ || exit 1
popd || exit 1

echo "${bold}\nAll tests completed successfully! 🥳\n${normal}"