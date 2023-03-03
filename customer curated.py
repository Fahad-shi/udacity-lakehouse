import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1677852138675 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677852138675",
)

# Script generated for node Join
Join_node1677852154061 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1677852138675,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1677852154061",
)

# Script generated for node accelerometer privacy
accelerometerprivacy_node2 = DropFields.apply(
    frame=Join_node1677852154061,
    paths=["user", "timeStamp", "x", "y"],
    transformation_ctx="accelerometerprivacy_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=accelerometerprivacy_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
