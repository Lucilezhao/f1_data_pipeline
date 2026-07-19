# Databricks notebook source
dbutils.wigets.text('p_batch_id', "")
v_batch_id=dbutils.wigets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00_common/01_environment_config

# COMMAND ----------

bronze_table=f'{catalog_name}.{bronze_schema}.circuits'
silver_table=f'{catalog_name}.{silver_schema}.circuits'

# COMMAND ----------

circuits_df=spark.table(bronze_table).filter(F.col("batch_id")==v_batch_id)
display(circuits_df)


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
    F.col('batch_id')
)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnsRenamed({
    "circuitId": "circuit_id",
    "circuitName": "circuit_name",
    "lat": "latitude",
    "long": "longitude"
})

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

circuits_final_df=(
    circuits_distinct_df
    .withColumn("circuit_name",F.initcap(F.col("circuit_name")))
    .withColumn("locality",F.initcap(F.col("locality")))

)

# COMMAND ----------

circuits_final_df=(
    circuits_final_df
    .withColumn("created_date",F.current_timestamp())
    .withColumn("updated_date",F.current_timestamp()
)

# COMMAND ----------

from delta.table import DeltaTable

if not DeltaTable.isDeltaTable(spark, silver_table):
    (
        circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
else:

    delta_table = DeltaTable.forName(spark, silver_table)
    (
        delta_table.alias("t")
        .merge(
            circuits_final_df.alias("s"),
            "t.circuit_id = s.circuit_id"
        )
        .whenMatchedUpdate(
            condition="s.batch_id >= t.batch_id",
            set={
                "circuit_name": "s.circuit_name",
                "latitude": "s.latitude",
                "longitude": "s.longitude",
                "locality": "s.locality",
                "country": "s.country",
                "ingestion_timestamp": "s.ingestion_timestamp",
                "source_file": "s.source_file",
                "batch_id": "s.batch_id",
                "updated_timestamp": "s.updated_timestamp"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

display(spark.table(silver_table))