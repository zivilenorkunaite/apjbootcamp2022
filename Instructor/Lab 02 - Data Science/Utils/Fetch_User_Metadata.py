# Databricks notebook source
# MAGIC %scala
# MAGIC spark.conf.set("com.databricks.training.module_name", "ap_juice")
# MAGIC val dbNamePrefix = {
# MAGIC   val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC   val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC   val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC   
# MAGIC   val username_final = username.split('@')(0)
# MAGIC   val module_name = spark.conf.get("com.databricks.training.module_name").toLowerCase()
# MAGIC 
# MAGIC   val databaseName = (username_final+"_"+module_name).replaceAll("[^a-zA-Z0-9]", "_") + "_db"
# MAGIC   spark.conf.set("com.databricks.training.spark.dbName", databaseName)
# MAGIC   spark.conf.set("com.databricks.training.spark.userName", username_final)
# MAGIC }

# COMMAND ----------

import os

# COMMAND ----------

databaseName = spark.conf.get("com.databricks.training.spark.dbName")
userName = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')

# COMMAND ----------

DATABASE_NAME = databaseName

# short version that we can use for databases, expriments, models etc
USERNAME = userName

# some weird voodo magic on getting scala object literal in python
# don't ask me how I figure it out
NOTEBOOK_PATH = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath()).replace('Some(', '').replace(')', '')

# root project path
PROJECT_PATH = '/'.join(NOTEBOOK_PATH.split('/')[:-1])

# COMMAND ----------

# response = USERNAME + " " + NOTEBOOK_PATH + " " + PROJECT_PATH + " " + DATABASE_NAME

# dbutils.notebook.exit(response)
