# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

# MAGIC %run ../00_common/02_bronze_helpers

# COMMAND ----------

source_file=f"{landing_folder_path}/sprints"
table_name=f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

sprints_schema = StructType([
    StructField("date", StringType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])

# COMMAND ----------

# Read data from the sprints file
sprints_df = (
    spark.read
        .format('json')
        .schema(sprints_schema)
        .option('mode', 'FAILFAST')
        .option('multiLine', True)
        .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

sprints_final_df=add_ingestion_metadata(sprints_df)

# COMMAND ----------

#write data to bronze table
(sprints_final_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))