# Databricks notebook source
from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df
        .withColumn('ingestion_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
    )


# COMMAND ----------

def write_to_bronze (
    input_df,
    target_table, 
    batch_id
    ):
    final_df = input_df.withColumn("batch_1id", F.lit(batch_ld))
(
    final_df
    .write
    .mode('overwrite')
    .format('delta')
    .partitionBy('batch_id')
    .option('replaceWhere', f"batch_id = '{batch_id}'")
    .saveAsTable('formula1.bronze.circuits')
)