# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

bronze_table=f'{catalog_name}.{bronze_schema}.drivers'
silver_table=f'{catalog_name}.{silver_schema}.drivers'

# COMMAND ----------

drivers_df=spark.table(bronze_table)
display(drivers_df)


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_dropped_df=drivers_df.drop("url")

# COMMAND ----------

drivers_renamed_df = drivers_dropped_df.withColumnsRenamed({
    "driverId": "drivers_id",
    "dateofBirth":"date_of_birth"
})

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

drivers_concatenate_df = (
    drivers_renamed_df
    .withColumn(
        "driver_name",
        F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyname"))
    )
    .drop("name")
)

# COMMAND ----------

# drivers_valid_df = drivers_renamed_df.filter(
#     F.col("drivers_id").isNotNull()
# )

drivers_distinct_df = drivers_concatenate_df.dropDuplicates(["drivers_id"])

# COMMAND ----------

drivers_final_df=(
    drivers_distinct_df
    .withColumn("nationality",F.initcap(F.col("nationality")))

)

# COMMAND ----------

(
    drivers_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "True")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))