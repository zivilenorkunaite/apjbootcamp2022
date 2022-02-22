# Databricks notebook source
# MAGIC %run ./Create_User_DB

# COMMAND ----------

# Get the email address entered by the user on the calling notebook
db_name = spark.conf.get("com.databricks.training.spark.dbName")

# Get user name

#username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#username_replaced = username.replace(".", "_").replace("@","_")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
#username = dbutils.widgets.get("user_name")
base_table_path = f"dbfs:/FileStore/{username}/deltademoasset/"
local_data_path = f"/dbfs/FileStore/{username}/deltademoasset/"

# Construct the unique database name
database_name = db_name
print(f"Database Name: {database_name}")

# DBFS Path is
print(f"DBFS Path is: {base_table_path}")

#Local Data path is
print(f"Local Data Path is: {local_data_path}")

temporary_dbfs_data_path = '/mnt/apjbootcamp/DATASETS/'

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

#donwload the file to local file path and move it to DBFS

import subprocess


# Delete local directories that may be present from a previous run

process = subprocess.Popen(['rm', '-f', '-r', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')




# COMMAND ----------

# Create local directories used in the workshop

process = subprocess.Popen(['mkdir', '-p', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# store incremental data as a table to generate extra files quicker

dataPath = f"{temporary_dbfs_data_path}/sales_202201.json"
df = spark.read.json(dataPath).createOrReplaceTempView('incremental_sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS apjbootcamp_working_db;
# MAGIC 
# MAGIC drop table if exists apjbootcamp_working_db.jan_sales;
# MAGIC 
# MAGIC create table apjbootcamp_working_db.jan_sales
# MAGIC using delta
# MAGIC as select *, from_unixtime(ts, "yyyy-MM-dd") as ts_date from incremental_sales order by from_unixtime(ts, "yyyy-MM-dd");

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)
