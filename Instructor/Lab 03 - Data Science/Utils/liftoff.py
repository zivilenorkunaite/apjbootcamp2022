# Databricks notebook source
# MAGIC %run "./Fetch_User_Metadata"

# COMMAND ----------

def run_setup(username, database, force_restart=False):
  # database exists
  database_exists = spark._jsparkSession.catalog().databaseExists(database)
  table_exists = spark._jsparkSession.catalog().tableExists(database, 'phytochemicals_quality')
  
  if (database_exists and table_exists) and not force_restart:
    pass
  else:
    setup_responses = dbutils.notebook.run("./Utils/setup-ds-gdrive", 0).split()
    #     dbfs_data_path = setup_responses[1]

    #     gold_table_path = f"{dbfs_data_path}tables/gold"
    #     dbutils.fs.rm(gold_table_path, recurse=True)

    #     dataPath = f"{dbfs_data_path}phytochemicals_quality.parquet"

    #     df = spark.read\
    #       .option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv(dataPath)

    #     df.write \
    #       .format("delta").mode("overwrite").saveAsTable(f"{database}.sensor_readings_historical_bronze")

    #   bronze_sample_exists = spark._jsparkSession.catalog().tableExists(database, 'sensor_readings_historical_bronze_sample')

    #   if not bronze_sample_exists:
    #     df = spark.sql(f'select * from {database}.sensor_readings_historical_bronze').sample(False, 0.05, 42)
    #     df.write.format('delta').mode('overwrite').saveAsTable(f'{database}.sensor_readings_historical_bronze_sample')    
    return setup_responses

# COMMAND ----------



# COMMAND ----------

response = run_setup(username=USERNAME, database=DATABASE_NAME)

# COMMAND ----------

displayHTML("""Database is <b style="color:green">{}</b>.""".format(databaseName))
displayHTML("""User name is <b style="color:green">{}</b>.""".format(userName))

if response is not None:
  displayHTML("""Local data path is <b style="color:green">{}</b>""".format(response[0]))
  displayHTML("""Base table path is <b style="color:green">{}</b>""".format(response[1]))

# COMMAND ----------


