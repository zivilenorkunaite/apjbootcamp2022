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
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/running-pipeline.png?raw=true" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Set up Environment

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Datasets", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]

dlt_ingest_path = f"{dbfs_data_path}/dlt_ingest/"

# COMMAND ----------

# MAGIC %run ./Utils/Define-Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare files for DLT
# MAGIC 
# MAGIC Run this once to reset all previous runs and prepare fresh dataset to be ingested

# COMMAND ----------

refresh_dlt_datasets = True

if refresh_dlt_datasets:
  dbutils.fs.rm(dlt_ingest_path, True)
  
  dbutils.fs.mkdirs(dlt_ingest_path)
  
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202112.json", dlt_ingest_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure DLT Pipeline
# MAGIC 
# MAGIC Pipeline code is stored in a different notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC You can find DLT Pipeline code notebook on Repos -> */apjbootcamp2022/Lab 01 - Data Engineering/04 - Delta Live Tables (SQL)*

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC You will also need a **Configuration**,  **Target** and  **Storage Location** values

# COMMAND ----------

storage_path = f'/tmp/{username}/dlt_pipeline'
dlt_database_name = f'{database_name}_dlt'


displayHTML("""<h2>Use these values to create your Delta Live Pipeline</h2>""")
displayHTML("""<b>Configuration:</b>""")
displayHTML("""Key: <b style="color:green">mypipeline.data_path</b>""")
displayHTML("""Value: <b style="color:green">{}</b>""".format(username))
displayHTML("""<b>Target:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format(dlt_database_name))
displayHTML("""<b>Storage Location: </b>""")
displayHTML("""<b style="color:green">{}</b>""".format(storage_path))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create and Run your Pipeline NOW
# MAGIC 
# MAGIC It will take some time for pipeline to start. While waiting - explore `04 - Delta Live Tables (SQL)` notebook to see the actual code used to create it.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Incremental Updates

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Simulate new batch files being uploaded to cloud location. 
# MAGIC 
# MAGIC If pipeline is running in continuous mode - files will be processed as soon as they are uploaded. Otherwise new files will be picked up on the next run.

# COMMAND ----------

get_incremental_data(dlt_ingest_path, 'SYD01','2022-01-01') 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Explore run Logs

# COMMAND ----------

spark.sql(f"USE {dlt_database_name};")

spark.sql(f"CREATE OR REPLACE VIEW pipeline_logs AS SELECT * FROM delta.`{storage_path}/system/events`")

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
