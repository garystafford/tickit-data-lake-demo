# Data Lake Demo

## References

- [Amazon Redshift TICKIT Sample Database](https://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html)

## TICKIT Tables
 - `tickit.saas.category`
 - `tickit.saas.event`
 - `tickit.saas.venue`
 - `tickit.crm.users`
 - `tickit.date`
 - `tickit.listing`
 - `tickit.sales`

## Demonstration Steps

Show in console, execute from command line.

1. Data Source
    1. Create (3) datasources and database with (7) tables (e.g., `tickit`)
    2. Download, cleanse, and import data to databases
    3. Run example SQL queries against PostgreSQL database including row count
2. Data Lake Resources
    1. Create Data Lake Amazon S3 bucket (e.g., `data-lake-111222333444-us-east-1`)
    2. Create AWS Glue Data Catalog (`tickit_demo`)
    3. Create S3 VPC Endpoint for next step
    4. Create (3) AWS Glue Connections (e.g., `tickit_postgresql`)
3. Source Data
    1. Create and run (3) Glue Crawler - creates (7) source tables (e.g., `tickit_postgresql`)
    2. Show CloudWatch logs
    3. Inspect AWS Glue Data Catalog with (7) tables
4. Bronze/Raw Data
    1. Create (7) Glue Jobs and run (`_convert`)
    2. Inspect AWS Glue Data Catalog with (14) tables
    3. Show data in S3
    4. Show Apache Avro data file
    5. Query Parquet-format bronze/raw data with Athena
5. Silver/Refined Data
    1. Create (7) Glue Jobs and run (`_refine`)
    2. Show Glue Jobs (e.g., `tickit_public_listing_refine` or `tickit_public_sales_refine`)
    3. Inspect AWS Glue Data Catalog with (21) tables
    4. Show Apache Parquet file(s) in S3
    5. Query Parquet-format silver/refined data with Athena
6. Gold/Aggregated Data
    1. Execute (2) CTAS in Athena to create two partitioned Gold datasets
    4. Show Apache Parquet file(s) in S3
    3. Query Parquet-format gold/aggregated data with Athena
    4. Save SELECT statement as view (e.g., `view_agg_tickit_sales_by_cat`)
7. Create View in AWS Glue data catalog using Athena
8. Show effectiveness of partitions
9. Show CloudFormation template

A view in Amazon Athena is a logical, not a physical table. The query that defines a view runs each time the view is referenced in a query.

## Commands

```shell
DATA_LAKE_BUCKET="your-bucket-us-east-1"

aws s3 rm "s3://${DATA_LAKE_BUCKET}/tickit/" --recursive

aws glue delete-database --name tickit_demo

echo '
{
  "DatabaseInput": {
        "Name": "tickit_demo",
        "Description": "Track sales activity for the fictional TICKIT web site"
  }
}' > glue_db.json
aws glue create-database --cli-input-json file://glue_db.json

aws glue get-tables \
  --database-name tickit_demo \
  --query 'TableList[].Name' \
  --output table

aws glue start-crawler --name tickit_postgresql
aws glue start-crawler --name tickit_mysql
aws glue start-crawler --name tickit_mssql

aws glue get-jobs \
  --query 'Jobs[].Name' \
  | sort | grep _convert

aws glue start-job-run --job-name tickit_public_category_convert
aws glue start-job-run --job-name tickit_public_date_convert
aws glue start-job-run --job-name tickit_public_event_convert
aws glue start-job-run --job-name tickit_public_listing_convert
aws glue start-job-run --job-name tickit_public_sales_convert
aws glue start-job-run --job-name tickit_public_users_convert
aws glue start-job-run --job-name tickit_public_venue_convert

aws glue start-job-run --job-name tickit_public_category_refine
aws glue start-job-run --job-name tickit_public_date_refine
aws glue start-job-run --job-name tickit_public_event_refine
aws glue start-job-run --job-name tickit_public_listing_refine
aws glue start-job-run --job-name tickit_public_sales_refine
aws glue start-job-run --job-name tickit_public_users_refine
aws glue start-job-run --job-name tickit_public_venue_refine

aws glue get-tables \
  --database-name tickit_demo \
  --query 'TableList[].Name' \
  --output table
```
