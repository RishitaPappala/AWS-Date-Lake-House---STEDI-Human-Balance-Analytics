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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step_Trainer_Trusted_Zone_Node
Step_Trainer_Trusted_Zone_Node_node1710856218408 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/step_trainer/trusted/"], "recurse": True}, transformation_ctx="Step_Trainer_Trusted_Zone_Node_node1710856218408")

# Script generated for node Accelerometer_Trusted_Zone_Node
Accelerometer_Trusted_Zone_Node_node1710856265327 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project-stedi-lake-house-rishi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometer_Trusted_Zone_Node_node1710856265327")

# Script generated for node SQL_Query_Join_&_Drop_Duplicates
SqlQuery226 = '''
select distinct * from step_trainer_trusted inner join accelerometer_trusted 
on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
'''
SQL_Query_Join__Drop_Duplicates_node1710856317262 = sparkSqlQuery(glueContext, query = SqlQuery226, mapping = {"step_trainer_trusted":Step_Trainer_Trusted_Zone_Node_node1710856218408, "accelerometer_trusted":Accelerometer_Trusted_Zone_Node_node1710856265327}, transformation_ctx = "SQL_Query_Join__Drop_Duplicates_node1710856317262")

# Script generated for node Machine_Learning_Curated
Machine_Learning_Curated_node1710856411960 = glueContext.getSink(path="s3://project-stedi-lake-house-rishi/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Machine_Learning_Curated_node1710856411960")
Machine_Learning_Curated_node1710856411960.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
Machine_Learning_Curated_node1710856411960.setFormat("json")
Machine_Learning_Curated_node1710856411960.writeFrame(SQL_Query_Join__Drop_Duplicates_node1710856317262)
job.commit()