import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Relational DB
RelationalDB_node1636205477123 = glueContext.create_dynamic_frame.from_catalog(
    database="tickit_demo",
    table_name="source_tickit_crm_users",
    transformation_ctx="RelationalDB_node1636205477123",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://open-data-lake-demo-us-east-1/tickit/bronze/users/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="tickit_demo", catalogTableName="raw_tickit_public_users"
)
S3bucket_node3.setFormat("avro")
S3bucket_node3.writeFrame(RelationalDB_node1636205477123)
job.commit()
