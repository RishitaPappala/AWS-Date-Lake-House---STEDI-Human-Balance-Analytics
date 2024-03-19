import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer_Landing_Zone_Node
Accelerometer_Landing_Zone_Node_node1710810832898 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_Landing_Zone_Node_node1710810832898")

# Script generated for node Customer_Trusted_Zone_Node
Customer_Trusted_Zone_Node_node1710810838544 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_Trusted_Zone_Node_node1710810838544")

# Script generated for node Join_Accelerometer_Landing_&_Customer_Trusted
Join_Accelerometer_Landing__Customer_Trusted_node1710810898913 = Join.apply(frame1=Accelerometer_Landing_Zone_Node_node1710810832898, frame2=Customer_Trusted_Zone_Node_node1710810838544, keys1=["user"], keys2=["email"], transformation_ctx="Join_Accelerometer_Landing__Customer_Trusted_node1710810898913")

# Script generated for node Drop_Fields_of_Customer_Trusted
Drop_Fields_of_Customer_Trusted_node1710810960745 = DropFields.apply(frame=Join_Accelerometer_Landing__Customer_Trusted_node1710810898913, paths=["serialNumber", "birthDay", "shareWithResearchAsOfDate", "registrationDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone", "shareWithPublicAsOfDate"], transformation_ctx="Drop_Fields_of_Customer_Trusted_node1710810960745")

# Script generated for node Accelerometer_Trusted_Zone_Node
Accelerometer_Trusted_Zone_Node_node1710811047254 = glueContext.getSink(path="s3://project-stedi-lake-house-rishi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_Trusted_Zone_Node_node1710811047254")
Accelerometer_Trusted_Zone_Node_node1710811047254.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="accelerometer_trusted")
Accelerometer_Trusted_Zone_Node_node1710811047254.setFormat("json")
Accelerometer_Trusted_Zone_Node_node1710811047254.writeFrame(Drop_Fields_of_Customer_Trusted_node1710810960745)
job.commit()