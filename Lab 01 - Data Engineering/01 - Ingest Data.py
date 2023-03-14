# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Prepare your lab
# MAGIC 
# MAGIC Run the next 2 cells to generate some data we will be using for this lab.
# MAGIC 
# MAGIC Data will be stored in a separate location

# COMMAND ----------

# MAGIC %run ./Utils/prepare-lab-environment

# COMMAND ----------

# This will take up to 2min to run
generate_sales_dataset()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest data from cloud storage
# MAGIC 
# MAGIC If your data is already in the cloud - you can simply read it from S3/ADLS 

# COMMAND ----------

products_cloud_storage_location = f'{datasets_location}products/products.json'
df = spark.read.json(products_cloud_storage_location)

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
# MAGIC ## Ingesting new files from same location
# MAGIC 
# MAGIC The `COPY INTO` SQL command lets you load data from a file location into a Delta table. This is a re-triable and idempotent operation; files in the source location that have already been loaded are skipped.

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS my_products;")

spark.sql(f"""
COPY INTO my_products 
FROM '{datasets_location}products/'
FILEFORMAT = json
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hands On Task!
# MAGIC 
# MAGIC We also have stores dataset available. Write COPY INTO statement for that dataset using `%sql` cell. 
# MAGIC 
# MAGIC `Hint` Use dbutils.fs.ls(datasets_location) to find sales dataset files and print that location to get full path for SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- CREATE TABLE IF NOT EXISTS my_storess;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Advanced Task
# MAGIC 
# MAGIC What would that look using autoloader? You can find syntax for it here: https://docs.databricks.com/getting-started/etl-quick-start.html

# COMMAND ----------

# Optional: write autoloader statement to load sales records

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingest data from API
# MAGIC 
# MAGIC If you want to query data via API you can use a python library requests and https://open-meteo.com/
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC We will need latitude and longitute for a given location. Look it up on https://www.latlong.net/ or use one of the examples:
# MAGIC   
# MAGIC   Auckland: 
# MAGIC   
# MAGIC     lat: -36.848461
# MAGIC     long: 174.763336
# MAGIC     
# MAGIC     
# MAGIC   Sydney:
# MAGIC   
# MAGIC     lat: -33.868820
# MAGIC     long: 151.209290

# COMMAND ----------

import requests
import json

# replace values with your chosen location
lat = -33.868820
long = 151.209290


today = datetime.datetime.now().strftime("%Y-%m-%d")
start_date =  pd.to_datetime(today) - pd.DateOffset(months=3) + pd.offsets.MonthBegin(-1)
end_date = pd.to_datetime(today)

url = f'https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={long}&start_date={start_date.strftime("%Y-%m-%d")}&end_date={end_date.strftime("%Y-%m-%d")}&hourly=temperature_2m,rain&timezone=auto'

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
# MAGIC `Hint`: Maybe switch to SQL and use some of the available SQL functions here https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin-alpha.html
# MAGIC 
# MAGIC 
# MAGIC `Hint 2`: Check out how `arrays_zip()` and `explode()` work

# COMMAND ----------

# Create a temperature over time visualisation

# COMMAND ----------

# Save this dataset as json file. We will be using it for our Transform part of the Lab

import datetime 

today = datetime.datetime.now()

unique_forecast_id = f"forecast{lat}{long}{today}"

df.write.mode('Overwrite').json(f"{datasets_location}weather/{unique_forecast_id}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Partner Connect
# MAGIC 
# MAGIC Partner Connect makes it easy for you to discover data, analytics and AI tools directly within the Databricks platform — and quickly integrate the tools you already use today. 
# MAGIC 
# MAGIC With Partner Connect, you can simplify tool integration to just a few clicks and rapidly expand the capabilities of your lakehouse.
