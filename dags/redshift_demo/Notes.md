# Notes

## References: Airflow

- <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/index.html>
- <https://github.com/apache/airflow/tree/main/airflow/providers/amazon/aws/example_dags>
- <https://github.com/airflow-plugins/Example-Airflow-DAGs/blob/master/etl/sftp_to_mongo.py>
- <https://github.com/apache/airflow/tree/main/airflow/example_dags>
- <https://www.astronomer.io/guides/managing-dependencies>
- <https://docs.aws.amazon.com/redshift/latest/dg/merge-examples.html>
-<https://www.astronomer.io/guides/error-notifications-in-airflow>

## References: Amazon Redshift

- <https://blog.satoricyber.com/hardening-aws-redshift-security-access-controls-explained>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html>
- <https://airflow.apache.org/docs/apache-airflow/1.10.1/howto/manage-connections.html#creating-a-connection-with-environment-variables>
- <https://registry.astronomer.io/dags/sql-check-redshift-etl>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD_command_examples.html>

```postgresql
CREATE SCHEMA tickit_demo;
CREATE GROUP airflow;
GRANT ALL ON SCHEMA tickit_demo to GROUP airflow;
GRANT ALL ON ALL TABLES IN SCHEMA tickit_demo TO GROUP airflow;
CREATE USER airflow PASSWORD 'AbCdEf123123';
ALTER GROUP airflow ADD USER airflow;


SELECT usename AS user_name, groname AS group_name 
FROM pg_user, pg_group
WHERE pg_user.usesysid = ANY(pg_group.grolist)
AND pg_group.groname in (SELECT DISTINCT pg_group.groname from pg_group);
```


```shell
gzip -c users.csv > users.gz
gzip -c venue.csv > venue.gz
gzip -c category.csv > category.gz
gzip -c date.csv > date.gz
gzip -c event.csv > event.gz
gzip -c listing.csv > listing.gz
gzip -c sales.csv > sales.gz
```

```text
redshift_demo__01_create_tables.py
Creates the tickit_demo named schema and seven tables that will hold the tickit data in Amazon Redshift

redshift_demo__02_initial_load.py
Creates seven stages tables then uses the COPY command to copy data from Amazon S3 to Redshift staging tables
Finally, uses the documented merge method to insert the data from the staging tables to the regular tables

Per AWS (https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html):
You can efficiently add new data to an existing table by using a combination of updates and inserts from a staging table. 
While Amazon Redshift does not support a single merge, or upsert, command to update a table from a single data source, 
you can perform a merge operation by creating a staging table and then using one of several methods described in the docs
update the target table from the staging table. We will use Merge method 1: Replacing existing rows.

redshift_demo__03_incremental_load.py
Similar to DAG #2, loads incremental (new) sales and listing data for the next sales period into Redshift

redshift_demo__04_unload_data.py
Joins and aggregates data from several tables and uploads the results to Amazon S3

redshift_demo__05_catalog_and_query.py
Catalogs the unloaded data in Amazon S3 using AWS Glue, then performs a test query in Amazon Athena

redshift_demo__06_run_dags_01_to_05.py
Sequentially triggers DAGS #1 through #5

redshift_demo__06B_run_dags_01_to_05.py
Alternate version of DAG 06. Uses external module for notifications.
```