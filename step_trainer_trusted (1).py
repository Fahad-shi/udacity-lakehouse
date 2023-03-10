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

# Script generated for node Drop Fields
DropFields_node1677967609996 = DropFields.apply(
    frame=AmazonS3_node1677880494461,
    paths=[
        "z",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1677967609996",
)

# Script generated for node Renamed keys for Step Trainer
RenamedkeysforStepTrainer_node1677880630655 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforStepTrainer_node1677880630655",
)

# Script generated for node Step Trainer
StepTrainer_node2 = Join.apply(
    frame1=DropFields_node1677967609996,
    frame2=RenamedkeysforStepTrainer_node1677880630655,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="StepTrainer_node2",
)

# Script generated for node Drop Fields
DropFields_node1677942721165 = DropFields.apply(
    frame=StepTrainer_node2,
    paths=["serialNumber"],
    transformation_ctx="DropFields_node1677942721165",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677942721165,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedilakehouse/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
