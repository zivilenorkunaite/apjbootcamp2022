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
# MAGIC   databaseName
# MAGIC }

# COMMAND ----------

database_name = spark.conf.get("com.databricks.training.spark.dbName")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')

displayHTML("""Username is <b style="color:green">{}</b>""".format(username))

# COMMAND ----------

base_table_path = f"dbfs:/FileStore/{username}/deltademoasset/"
local_data_path = f"/dbfs/FileStore/{username}/deltademoasset/"
