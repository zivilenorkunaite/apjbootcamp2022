# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Delta Live Tables (DLT) makes it easy to build and manage reliable data pipelines that deliver high quality data on Delta Lake. 
# MAGIC 
# MAGIC DLT helps data engineering teams simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/09/Live-Tables-Pipeline.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/05/Pipeline-Graph.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Set up Environment

# COMMAND ----------

# MAGIC %run "./Includes/env_setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare files for DLT
# MAGIC 
# MAGIC Run this once to reset all previous runs

# COMMAND ----------


updateFilesLocation = f"{s3_location}/data/updates/fin_transactions_update.csv/"
dltFilesLocation = f"{s3_location}/data/dlt/fin_transactions/"

 
# delete source files to have a clean starting point
#dbutils.fs.rm(dltFilesLocation, recurse = True)
#dbutils.fs.mkdirs(dltFilesLocation)
#dbutils.fs.cp(updateFilesLocation, dltFilesLocation, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Prepare and Run DLT Pipeline
# MAGIC 
# MAGIC Pipeline code is stored in a different notebook

# COMMAND ----------

# MAGIC %run "./04 - Delta Live Tables (SQL)"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Simulate new batch files being uploaded to S3 location. If pipeline is running in continues mode - files will be processed as soon as they are uploaded to given S3 bucket

# COMMAND ----------

#dbutils.fs.cp(f"{s3_location}/data/updates/fin_transactions_update_2.csv/", dltFilesLocation, True)

#dbutils.fs.cp(f"{s3_location}/data/updates/fin_transactions_update_3.csv/", dltFilesLocation, True)

dbutils.fs.cp(f"{s3_location}/data/updates/fin_transactions_update_4.csv/", dltFilesLocation, True)

# COMMAND ----------

# DBTITLE 1,Variables defined as per Delta Live Tables Pipeline Configuration
storage_path = '/temp/zivile.norkunaite/dlt/zivile_dtl_fin_demo-1115'
dlt_database = 'zivile_fin_demo_dlt'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explore run Logs

# COMMAND ----------

spark.sql(f"USE {dlt_database};")

spark.sql(f"CREATE OR REPLACE VIEW pipeline_logs AS SELECT * FROM delta.`{storage_path}/system/events`")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM pipeline_logs
# MAGIC ORDER BY timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC * `user_action` Events occur when taking actions like creating the pipeline
# MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
# MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
# MAGIC   * `flow_type` - whether this is a complete or append flow
# MAGIC   * `explain_text` - the Spark explain plan
# MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
# MAGIC   * `metrics` - currently contains `num_output_rows`
# MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
# MAGIC     * `dropped_records`
# MAGIC     * `expectations`
# MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   details:flow_definition.output_dataset,
# MAGIC   details:flow_definition.input_datasets,
# MAGIC   details:flow_definition.flow_type,
# MAGIC   details:flow_definition.schema,
# MAGIC   details:flow_definition
# MAGIC FROM pipeline_logs
# MAGIC WHERE details:flow_definition IS NOT NULL
# MAGIC ORDER BY timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   id,
# MAGIC   expectations.dataset,
# MAGIC   expectations.name,
# MAGIC   expectations.failed_records,
# MAGIC   expectations.passed_records
# MAGIC FROM(
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
# MAGIC              ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
# MAGIC   FROM pipeline_logs
# MAGIC   WHERE details:flow_progress.metrics IS NOT NULL) data_quality
