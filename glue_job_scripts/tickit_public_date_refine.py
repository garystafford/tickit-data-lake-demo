import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


# Script generated for node Custom transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import when, col

    newdf = dfc.select(list(dfc.keys())[0]).toDF()
    newdf = newdf.withColumn(
        "month_int",
        when(col("month") == "JAN", 1)
            .when(col("month") == "FEB", 2)
            .when(col("month") == "MAR", 3)
            .when(col("month") == "APR", 4)
            .when(col("month") == "MAY", 5)
            .when(col("month") == "JUN", 6)
            .when(col("month") == "JUL", 7)
            .when(col("month") == "AUG", 8)
            .when(col("month") == "SEP", 9)
            .when(col("month") == "OCT", 10)
            .when(col("month") == "NOV", 11)
            .when(col("month") == "DEC", 12),
    )

    newdatedata = DynamicFrame.fromDF(newdf, glueContext, "newdatedata")
    return DynamicFrameCollection({"CustomTransform0": newdatedata}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="tickit_demo",
    table_name="raw_tickit_public_date",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Custom transform
Customtransform_node1636852235381 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"DataCatalogtable_node1": DataCatalogtable_node1}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1636853203313 = SelectFromCollection.apply(
    dfc=Customtransform_node1636852235381,
    key=list(Customtransform_node1636852235381.keys())[0],
    transformation_ctx="SelectFromCollection_node1636853203313",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://open-data-lake-demo-us-east-1/tickit/silver/date_new/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="tickit_demo", catalogTableName="refined_tickit_public_date_new"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(SelectFromCollection_node1636853203313)
job.commit()
