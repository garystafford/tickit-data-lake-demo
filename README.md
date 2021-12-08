# DataOp AWS Video Demonstrations and Blog Posts: Source Code Resources

## Overview

[![Test DAGs](https://github.com/garystafford/tickit-data-lake-demo/actions/workflows/test_dags.yml/badge.svg?branch=main)](https://github.com/garystafford/tickit-data-lake-demo/actions/workflows/test_dags.yml)

[![Upload DAGs](https://github.com/garystafford/tickit-data-lake-demo/actions/workflows/upload_dags.yml/badge.svg?branch=main)](https://github.com/garystafford/tickit-data-lake-demo/actions/workflows/upload_dags.yml)

Source code for video demonstration detailed in the
post, [Building a Simple Data Lake on AWS](https://garystafford.medium.com/building-a-simple-data-lake-on-aws-df21ca092e32)
. Build a simple data lake on AWS using a combination of services, including Amazon MWAA, AWS Glue Data Catalog, AWS
Glue Crawlers, AWS Glue Jobs, AWS Glue Studio, Amazon Athena, and Amazon S3.

Source code for video demonstration detailed in the
post, [Building a Data Lake with Apache Airflow](https://garystafford.medium.com/building-a-data-lake-with-apache-airflow-b48bd953c2b)
. Programmatically build a simple Data Lake on AWS using Amazon Managed Workflows for Apache Airflow, AWS Glue, and Amazon Athena.

Source code for video demonstration detailed in the
post, [AWS Lakehouse Automation](https://medium.com/@garystafford/aws-lakehouse-automation)
. Programmatically load data into and upload data from Amazon Redshift using Apache Airflow.

## Architecture

![Architecture](./diagram/diagram.png)

## TICKIT Sample Database

[Amazon Redshift TICKIT Sample Database](https://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html)

## TICKIT Tables

- `tickit.saas.category`
- `tickit.saas.event`
- `tickit.saas.venue`
- `tickit.crm.users`
- `tickit.date`
- `tickit.listing`
- `tickit.sales`

## Naming Conventions

```text
+-------------+--------------------------------------------------------------------+
| Prefix      | Description                                                        |
+-------------+--------------------------------------------------------------------+
| _source     | Data Source metadata only (org. call _raw in video)                |
| _raw        | Raw/Bronze data from data sources (org. call _converted in video)  |
| _refined    | Refined/Silver data - raw data with initial ELT/cleansing applied  |
| _aggregated | Gold/Aggregated data - aggregated/joined refined data              |
+-------------+--------------------------------------------------------------------+
```

## AWS CLI Commands

There were two small changes made to the source code, as compared to the video demonstration, to help clarify the flow
of data in the demonstration. The prefix for the (7) data source AWS Glue Data Catalog table’s prefix was switched
from `raw_` from `source_`. Also, the (7) Raw/Bronze AWS Glue Data Catalog table’s prefix was switched from `converted_`
to `raw_`. The final data flow is 1) `source_`, 2) `raw_`, 3) `refined_`, and 4) `agg_` (aggregated).

```shell
DATA_LAKE_BUCKET="your-data-lake-bucket"

aws s3 rm "s3://${DATA_LAKE_BUCKET}/tickit/" --recursive

aws glue delete-database --name tickit_demo

aws glue create-database \
  --database-input '{"Name": "tickit_demo", "Description": "Track sales activity for the fictional TICKIT web site"}'

aws glue get-tables \
  --database-name tickit_demo \
  --query "TableList[].Name" \
  --output table

aws glue start-crawler --name tickit_postgresql
aws glue start-crawler --name tickit_mysql
aws glue start-crawler --name tickit_mssql

aws glue get-tables \
  --database-name tickit_demo \
  --query "TableList[].Name" \
  --expression "source_*"  \
  --output table

aws glue start-job-run --job-name tickit_public_category_raw
aws glue start-job-run --job-name tickit_public_date_raw
aws glue start-job-run --job-name tickit_public_event_raw
aws glue start-job-run --job-name tickit_public_listing_raw
aws glue start-job-run --job-name tickit_public_sales_raw
aws glue start-job-run --job-name tickit_public_users_raw
aws glue start-job-run --job-name tickit_public_venue_raw

aws glue start-job-run --job-name tickit_public_category_refine
aws glue start-job-run --job-name tickit_public_date_refine
aws glue start-job-run --job-name tickit_public_event_refine
aws glue start-job-run --job-name tickit_public_listing_refine
aws glue start-job-run --job-name tickit_public_sales_refine
aws glue start-job-run --job-name tickit_public_users_refine
aws glue start-job-run --job-name tickit_public_venue_refine

aws glue get-tables \
  --database-name tickit_demo \
  --query "TableList[].Name" \
  --output table

aws s3api list-objects-v2 \
  --bucket ${DATA_LAKE_BUCKET} \
  --prefix "tickit/" \
  --query "Contents[].Key" \
  --output table
```
