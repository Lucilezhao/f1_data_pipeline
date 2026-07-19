# Databricks notebook source
# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

bronze_table=f'{catalog_name}.{bronze_schema}.constructors'
silver_table=f'{catalog_name}.{silver_schema}.constructors'

# COMMAND ----------

constructors_df=spark.table(bronze_table)
display(constructors_df)


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnsRenamed({
    "ConstructorId": "constructor_id",
    "Name": "constructor_name"
})

# COMMAND ----------

# constructors_valid_df = constructors_renamed_df.filter(
#     F.col("constructors_id").isNotNull()
# )

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

constructors_final_df=(
    constructors_distinct_df
    .withColumn("nationality",F.initcap(F.col("nationality")))

)

# COMMAND ----------

(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))