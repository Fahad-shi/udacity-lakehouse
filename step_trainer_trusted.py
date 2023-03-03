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

# Script generated for node Amazon S3
AmazonS3_node1677880494461 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1677880494461",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedilakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Renamed keys for Step Trainer
RenamedkeysforStepTrainer_node1677880630655 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforStepTrainer_node1677880630655",
)

# Script generated for node Step Trainer
StepTrainer_node2 = Join.apply(
    frame1=RenamedkeysforStepTrainer_node1677880630655,
    frame2=AmazonS3_node1677880494461,
    keys1=["`(right) serialNumber`"],
    keys2=["serialNumber"],
    transformation_ctx="StepTrainer_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=StepTrainer_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
