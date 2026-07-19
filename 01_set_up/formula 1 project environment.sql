-- Databricks notebook source
-- MAGIC %md
-- MAGIC # set up the project environment for formula project
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC access cloud storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@dbcourseextdl11.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create external location

-- COMMAND ----------

create external location if not exists databricks_course_ext_dl1_formula1_incr
url 'abfss://formula1-incr@dbcourseextdl11.dfs.core.windows.net/'
with (storage credential databricks_course_sc)
comment 'external location for of formula1 containers'

-- COMMAND ----------

show catalogs

-- COMMAND ----------

CREATE CATALOG  IF NOT EXISTS  formula1_incr
    MANAGED LOCATION 'abfss://formula1-incr@dbcourseextdl11.dfs.core.windows.net/' 
    COMMENT 'main catalog of formula project';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1@dbcourseextdl11.dfs.core.windows.net/bronze'; 
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1@dbcourseextdl11.dfs.core.windows.net/silver'; 
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1@dbcourseextdl11.dfs.core.windows.net/gold'; 

-- COMMAND ----------

use catalog formula1;
show schemas

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@dbcourseextdl11.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files