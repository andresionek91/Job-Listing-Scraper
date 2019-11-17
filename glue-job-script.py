import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

glue_source_database = "glassdoor"
glue_source_table = "glassdoor"
glue_temp_storage = "s3://s3-glassdoor-glue-temp/glassdoor/"
output_path = "s3://s3-kaggle-survey-glassdoor/glassdoor_prepared/glassdoor{}"
root_table_name = "root"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=glue_source_database,
    table_name=glue_source_table,
    transformation_ctx="datasource")

dropnullfields = DropNullFields.apply(
    frame=datasource,
    transformation_ctx="dropnullfields")

unnested = Relationalize.apply(
    frame=dropnullfields,
    staging_path=glue_temp_storage,
    name=root_table_name,
    transformation_ctx="unnested")

for df_name in unnested.keys():
    df = unnested.select(df_name)

    df_dropnullfields = DropNullFields.apply(
        frame=df,
        transformation_ctx="df_dropnullfields")

    table_name = df_name.replace(root_table_name, '').replace('.', '_')
    print("Writing to table: ", table_name)

    conn_options = {
        "path": output_path.format(table_name),
    }

    partitioned_dataframe = df_dropnullfields.toDF().repartition(1)
    partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

    datasink = glueContext.write_dynamic_frame.from_options(
        frame=partitioned_dynamicframe,
        connection_type="s3",
        connection_options=conn_options,
        format="csv",
        transformation_ctx="datasink"
    )

job.commit()
