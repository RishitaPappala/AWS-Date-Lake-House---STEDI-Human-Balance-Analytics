import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer_Trusted_Zone_Node
Customer_Trusted_Zone_Node_node1710811629432 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_Trusted_Zone_Node_node1710811629432")

# Script generated for node Accelerometer_Trusted_Zone_Node
Accelerometer_Trusted_Zone_Node_node1710811631699 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometer_Trusted_Zone_Node_node1710811631699")

# Script generated for node Join_Customer_Trusted_&_Accelerometer_Trusted
Join_Customer_Trusted__Accelerometer_Trusted_node1710811806943 = Join.apply(frame1=Customer_Trusted_Zone_Node_node1710811629432, frame2=Accelerometer_Trusted_Zone_Node_node1710811631699, keys1=["email"], keys2=["user"], transformation_ctx="Join_Customer_Trusted__Accelerometer_Trusted_node1710811806943")

# Script generated for node Drop_Fields_of_Accelerometer_Trusted
Drop_Fields_of_Accelerometer_Trusted_node1710811871196 = DropFields.apply(frame=Join_Customer_Trusted__Accelerometer_Trusted_node1710811806943, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="Drop_Fields_of_Accelerometer_Trusted_node1710811871196")

# Script generated for node Drop_Duplicates
Drop_Duplicates_node1710811974185 =  DynamicFrame.fromDF(Drop_Fields_of_Accelerometer_Trusted_node1710811871196.toDF().dropDuplicates(), glueContext, "Drop_Duplicates_node1710811974185")

# Script generated for node Customer_Curated_Zone_Node
Customer_Curated_Zone_Node_node1710812021458 = glueContext.getSink(path="s3://project-stedi-lake-house-rishi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Curated_Zone_Node_node1710812021458")
Customer_Curated_Zone_Node_node1710812021458.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_curated")
Customer_Curated_Zone_Node_node1710812021458.setFormat("json")
Customer_Curated_Zone_Node_node1710812021458.writeFrame(Drop_Duplicates_node1710811974185)
job.commit()