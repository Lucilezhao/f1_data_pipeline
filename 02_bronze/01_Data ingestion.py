# Databricks notebook source
dbutils.wigets.text('p_batch_id', "")
v_batch_id=dbutils.wigets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

# MAGIC %run ../00_common/02_bronze_helpers

# COMMAND ----------

catalog_name

# COMMAND ----------

source_file_circuits=f"{landing_folder_path}/{v_batch_id}/circuits.csv"
source_file_races=f"{landing_folder_path}/races.csv"
# can replace hard code '/Volumes/formula1/landing/files/circuits.csv' with 'source_file'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, StringType

StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)
])


# COMMAND ----------

circuits_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .option('inferSchema', True)
        .load(source_file_circuits)
)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql import functions as f
circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

# circuits_final_df=circuits_final_df.withColumn("batch_id",F.lit(v_batch_id))
# (
#     circuits_final_df
#     .write
#     .mode('overwrite')
#     .format('delta')
#     .partitionBy('batch_id')
#     .option('replaceWhere', f"batch_id = '{v_batch_id}'")
#     .saveAsTable('formula1.bronze.circuits')
# )

# COMMAND ----------

write_to_bronze (
    input_df=circuits_final_df
    target_table=table_name
    batch_id=v_batch_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ##races file
# MAGIC

# COMMAND ----------

races_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .option('inferSchema', True)
        .load('/Volumes/formula1/landing/files/races.csv')
)

# COMMAND ----------

display(races_df)
races_final_df = add_ingestion_metadata(races_df)
races_final_df.write.mode('overwrite').format('delta').saveAsTable('formula1.bronze.races')
display(spark.read.table('formula1.bronze.races'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.races

# COMMAND ----------

# MAGIC %md
# MAGIC ##