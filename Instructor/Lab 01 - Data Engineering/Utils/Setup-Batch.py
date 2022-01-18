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

import urllib

linkToFile = 'https://apjbootcamp.blob.core.windows.net/datasets/apj_locations.csv?sp=r&st=2022-01-17T02:29:08Z&se=2022-01-20T10:29:08Z&spr=https&sv=2020-08-04&sr=b&sig=os%2F4ngK%2BlT1zj3NAvHz3K5bT%2B2QJU5QQGhM%2BzrQ8cW4%3D'
localDestination = f'{local_data_path}apj_locations.csv'

resultFilePath, responseHeaders = urllib.request.urlretrieve(linkToFile, localDestination)

# COMMAND ----------

dataPath1 = f'{base_table_path}apj_locations.csv'

df1 = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath1)

display(df1)

# COMMAND ----------

# UPDATE THE REST FROM HERE

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)
