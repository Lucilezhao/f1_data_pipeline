# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

bronze_table=f'{catalog_name}.{bronze_schema}.results'
silver_table=f'{catalog_name}.{silver_schema}.results'

# COMMAND ----------

results_df=spark.table(bronze_table)
display(results_df)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_dropped_df=results_df.drop("url")

# COMMAND ----------

results_renamed_df = results_dropped_df.withColumnsRenamed({
    "constructorId": "constructor_id",
    "driverId": "driver_id",
    "raceName": "race_name",
    "positionText": "finish_position_text",
    "date": "race_date",
    "grid": "grid_position",
    "laps": "completed_laps",
    "number": "car_number",
    "position": "finish_position"
})

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

results_valid_df = results_renamed_df.filter(
    F.col("season").isNotNull()
    & F.col("round").isNotNull()
    & F.col("constructor_id").isNotNull()
    & F.col("driver_id").isNotNull()
)

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates()

# COMMAND ----------

results_final_df=(
    results_distinct_df
    .withColumn("race_name",F.initcap(F.col("race_name")))
)

# COMMAND ----------

(
    results_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))