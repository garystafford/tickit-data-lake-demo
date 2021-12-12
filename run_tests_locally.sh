#!/bin/sh

# Purpose: Run Airflow DAG tests locally before committing and pushing
# Author: Gary A. Stafford
# Modified: 2021-12-11
# Run this command first:
# python3 -m pip install --user -U -r requirements_local_tests.txt

bold=$(tput bold)
normal=$(tput sgr0)

echo "\nStarting Flake8 test..."
flake8 --ignore E501 dags --benchmark || exit 1

echo "\nStarting Black test..."
python3 -m pytest --cache-clear
python3 -m pytest dags/ --black -v || exit 1

echo "\nStarting Pytest tests..."
cd tests || exit
python3 -m pytest tests.py -v || exit 1

echo "${bold}\nAll tests completed successfully! 🥳\n${normal}"