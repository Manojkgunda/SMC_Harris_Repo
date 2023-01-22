# Databricks notebook source
# MAGIC %sql SET spark.databricks.delta.formatCheck.enabled=false;

# COMMAND ----------


luth_source_location = "s3://harris-data/luth-data/luth_device_reference/"
file_type = "parquet"

df_luth_device_reference = spark.read.format(file_type).load(luth_source_location)

display(df_luth_device_reference)

# COMMAND ----------

df_luth_device_reference.createOrReplaceTempView("luth_device_reference")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   distinct month_key
# MAGIC from
# MAGIC   luth_device_reference
# MAGIC order by
# MAGIC   1;

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS luth_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an external table
# MAGIC DROP TABLE IF EXISTS luth_data.luth_device_reference; 
# MAGIC CREATE TABLE luth_data.luth_device_reference
# MAGIC USING delta
# MAGIC SELECT * FROM luth_device_reference

# COMMAND ----------

# MAGIC %sql
# MAGIC use luth_data;
# MAGIC select distinct month_key from luth_data.luth_device_reference order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC convert to delta parquet.`s3://harris-data/luth-data/luth_device_reference/` PARTITIONED BY (month_key int)

# COMMAND ----------

# MAGIC %sql
# MAGIC use luth_data;
# MAGIC CREATE TABLE luth_device_reference1 AS
# MAGIC SELECT * FROM delta.`s3://harris-data/luth-data/luth_device_reference/`

# COMMAND ----------

# MAGIC %sql
# MAGIC use luth_data;
# MAGIC select distinct month_key from luth_device_reference1;

# COMMAND ----------

spark.catalog.listTables('luth_data')

# COMMAND ----------

df_luth_device_reference.write.format("delta").mode("overwrite").option("path", "s3://harris-data/luth-data/luth_device_reference/").saveAsTable("luth_data.luth_device_reference2");

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


