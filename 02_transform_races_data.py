# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

bronze_table=f'{catalog_name}.{bronze_schema}.races'
silver_table=f'{catalog_name}.{silver_schema}.races'

# COMMAND ----------

races_df=spark.table(bronze_table)
display(races_df)


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"), 
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnsRenamed({
    "raceName": "race_name",
    "date": "race_date",
    "circuitId": "circuit_id"
})

# COMMAND ----------

# races_valid_df = races_renamed_df.filter(
#     F.col("races_id").isNotNull()
# )

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

races_final_df=(
    races_distinct_df
    .withColumn("race_name",F.initcap(F.col("race_name")))

)

# COMMAND ----------

(
    races_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))