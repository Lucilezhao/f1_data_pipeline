# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

# MAGIC %run ../00_common/02_bronze_helpers

# COMMAND ----------

source_file=f"{landing_folder_path}/constructors.json"
table_name=f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

#define shcema
constructors_schema="""
  constructorId string,
  url string,
  name string,
  nationality string
  """

# COMMAND ----------

#read data from constructors file
constructors_df = (
    spark
    .read
    .format('json')
    .schema(constructors_schema)
    .option('mode','failfast')
    .load(source_file)
)


# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_final_df=add_ingestion_metadata(constructors_df)

# COMMAND ----------

#write data to bronze table
(constructors_final_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))