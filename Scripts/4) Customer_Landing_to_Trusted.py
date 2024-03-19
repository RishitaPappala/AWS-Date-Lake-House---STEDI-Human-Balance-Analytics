import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer_Landing_Zone_Node
Customer_Landing_Zone_Node_node1710798209274 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/customer/landing/"], "recurse": True}, transformation_ctx="Customer_Landing_Zone_Node_node1710798209274")

# Script generated for node Research_Filter
Research_Filter_node1710798301256 = Filter.apply(frame=Customer_Landing_Zone_Node_node1710798209274, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Research_Filter_node1710798301256")

# Script generated for node Customer_Trusted_Zone_Node
Customer_Trusted_Zone_Node_node1710798602141 = glueContext.getSink(path="s3://project-stedi-lake-house-rishi/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Trusted_Zone_Node_node1710798602141")
Customer_Trusted_Zone_Node_node1710798602141.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
Customer_Trusted_Zone_Node_node1710798602141.setFormat("json")
Customer_Trusted_Zone_Node_node1710798602141.writeFrame(Research_Filter_node1710798301256)
job.commit()