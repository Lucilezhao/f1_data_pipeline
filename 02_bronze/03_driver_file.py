# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

# MAGIC %run ../00_common/02_bronze_helpers

# COMMAND ----------

source_file=f"{landing_folder_path}/drivers.json"
table_name=f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,datetime
name_schema=StructType([
    StructField('givenName', StringType()),
    StructField('familyname', StringType())
])
driver_schema=StructType([
    StructField('driverId',StringType()), 
    StructField('name' ,name_schema),      
    StructField('url', StringType()),
    StructField('dateOfBirth', StringType()),
    StructField('nationality', StringType())]
)



# COMMAND ----------

#read data from drivers file
drivers_df = (
    spark
    .read
    .format('json')
    .schema(driver_schema)
    .option('mode','failfast')
    .load(source_file)
)


# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_final_df=add_ingestion_metadata(drivers_df)

# COMMAND ----------

#write data to bronze table
(drivers_final_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))