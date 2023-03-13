# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC If your data is already in the cloud - you can simply read it from S3/ADLS 

# COMMAND ----------

cloud_storage_path = '/FileStore/tmp/databricksbootcamp/datasets/products/products.json'
df = spark.read.json(cloud_storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hands On Task!
# MAGIC 
# MAGIC Do you remember how to explore this dataset using notebooks?
# MAGIC 
# MAGIC Hint: use display() or createOrReplaceTemporaryView()

# COMMAND ----------

# Explore customers dataset


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## TODO
# MAGIC Using COPY INTO

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If you want to query data via API you can use a python library requests and https://open-meteo.com/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We will need latitude and longitute for a given location. Look it up on https://www.latlong.net/ or use one of the examples:
# MAGIC   
# MAGIC   Auckland: 
# MAGIC     lat: -36.848461
# MAGIC     long: 174.763336
# MAGIC       
# MAGIC   Sydney:
# MAGIC     lat: -33.868820
# MAGIC     long: 151.209290

# COMMAND ----------

import requests
import json

# replace values with your chosen location
lat = -33.868820
long = 151.209290


url = f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&current_weather=true&hourly=temperature_2m,rain&timezone=auto'

response = requests.get(url)

if response.status_code == 200:
  json_data = sc.parallelize([response.text])
  df = spark.read.json(json_data)
  df.display()

else:
  print('Check your URL for errors!')
  print(response.reason)



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hands On Task
# MAGIC 
# MAGIC Can you draw a temprature chart using this dataset?
# MAGIC 
# MAGIC Hint: Maybe switch to SQL and use some of the available SQL functions here https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin-alpha.html

# COMMAND ----------

# Create a temperature over time visualisation

# COMMAND ----------

# Save this dataset as json file. We will be using it for our Transform part of the Lab

import datetime 

current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

datasets_location = f'/tmp/{current_user_id}/datasets/weather'
today = datetime.datetime.now()

unique_forecast_id = f"forecast{lat}{long}{today}"

df.write.mode('Overwrite').json(f"{datasets_location}/{unique_forecast_id}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## TODO
# MAGIC 
# MAGIC Partner connect demo
