import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Curated_Zone_Node
Customer_Curated_Zone_Node_node1710855212176 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://project-stedi-lake-house-rishi/customer/curated/"],
            "recurse": True,
        },
        transformation_ctx="Customer_Curated_Zone_Node_node1710855212176",
    )
)

# Script generated for node Step_Trainer_Landing_Zone_Node
Step_Trainer_Landing_Zone_Node_node1710855170816 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://project-stedi-lake-house-rishi/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="Step_Trainer_Landing_Zone_Node_node1710855170816",
    )
)

# Script generated for node SQL_Query_Join_&_Drop_Duplicates
SqlQuery602 = """
select distinct step_trainer_landing.sensorReadingTime, step_trainer_landing.serialNumber, step_trainer_landing.distanceFromObject
from step_trainer_landing
inner join customer_curated on customer_curated.serialnumber = step_trainer_landing.serialnumber
"""
SQL_Query_Join__Drop_Duplicates_node1710855414556 = sparkSqlQuery(
    glueContext,
    query=SqlQuery602,
    mapping={
        "step_trainer_landing": Step_Trainer_Landing_Zone_Node_node1710855170816,
        "customer_curated": Customer_Curated_Zone_Node_node1710855212176,
    },
    transformation_ctx="SQL_Query_Join__Drop_Duplicates_node1710855414556",
)

# Script generated for node Step_Trainer_Trusted_Zone_Ndde
Step_Trainer_Trusted_Zone_Ndde_node1710855625819 = glueContext.getSink(
    path="s3://project-stedi-lake-house-rishi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Step_Trainer_Trusted_Zone_Ndde_node1710855625819",
)
Step_Trainer_Trusted_Zone_Ndde_node1710855625819.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="step_trainer_trusted"
)
Step_Trainer_Trusted_Zone_Ndde_node1710855625819.setFormat("json")
Step_Trainer_Trusted_Zone_Ndde_node1710855625819.writeFrame(
    SQL_Query_Join__Drop_Duplicates_node1710855414556
)
job.commit()
