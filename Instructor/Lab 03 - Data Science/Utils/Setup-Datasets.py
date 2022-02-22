# Databricks notebook source
# MAGIC %run ./Fetch_User_Metadata

# COMMAND ----------

database_name = DATABASE_NAME

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

!pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib tqdm

# COMMAND ----------

import re
import io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import requests
from tqdm import tqdm


def download_file_from_google_drive(id, destination):
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    def save_response_content(response, destination):
        CHUNK_SIZE = 32768
        # get the file size from Content-length response header
        file_size = int(response.headers.get("Content-Length", 0))
        # extract Content disposition from response headers
        content_disposition = response.headers.get("content-disposition")
        # parse filename
        filename = re.findall("filename=\"(.+)\"", content_disposition)[0]
        print("[+] File size:", file_size)
        print("[+] File name:", filename)
        progress = tqdm(response.iter_content(CHUNK_SIZE), f"Downloading {filename} to {destination}", total=file_size, unit="Byte", unit_scale=True, unit_divisor=1024)
        with open(destination, "wb+") as f:
            for chunk in progress:
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    # update the progress bar
                    progress.update(len(chunk))
        progress.close()

    # base URL for download
    URL = "https://docs.google.com/uc?export=download"
    # init a HTTP session
    session = requests.Session()
    # make a request
    response = session.get(URL, params = {'id': id}, stream=True)
    print("[+] Downloading", response.url)
    # get confirmation token
    token = get_confirm_token(response)
    if token:
        params = {'id': id, 'confirm':token}
        response = session.get(URL, params=params, stream=True)
    # download to disk
    save_response_content(response, destination)  

# COMMAND ----------

def download_datasets(datasets_data_path, full_refresh=False):
  
  all_datasets_with_file_id = {
    "orange_qualities.parquet": "1boLrpoOwS1OtO_ynaellTNThzY2BuDtn"
  }
  
  if full_refresh:
    dbutils.fs.rm(datasets_data_path, True)
  
  dbutils.fs.mkdirs(datasets_data_path)
  
  existing_datasets = [f.name for f in dbutils.fs.ls(datasets_data_path.replace('/dbfs/','dbfs:/'))]
  print(existing_datasets)

  # for each file check if it already exists and only download if not. Check by filename only
  for dataset_name in all_datasets_with_file_id:
    if not (dataset_name in existing_datasets):
      download_file_from_google_drive(all_datasets_with_file_id[dataset_name], f"{datasets_data_path}{dataset_name}")
  

# COMMAND ----------

database_exists = spark._jsparkSession.catalog().databaseExists(DATABASE_NAME)
table_exists = spark._jsparkSession.catalog().tableExists(DATABASE_NAME, 'phytochemicals_quality')
  

# COMMAND ----------



# COMMAND ----------

def run_setup(username, database, force_restart=False):
  # database exists
  database_exists = spark._jsparkSession.catalog().databaseExists(database)
  bronze_exists = spark._jsparkSession.catalog().tableExists(database, 'sensor_readings_historical_bronze')
  
  if (database_exists and bronze_exists) and not force_restart:
    pass
  else:
    setup_responses = dbutils.notebook.run("../Lab 1: Data Engineering/Utils/Setup-Batch-GDrive", 0, {"db_name": username}).split()
    dbfs_data_path = setup_responses[1]
    
    bronze_table_path = f"{dbfs_data_path}tables/bronze"
    silver_table_path = f"{dbfs_data_path}tables/silver"
    gold_table_path = f"{dbfs_data_path}tables/gold"
    
    dbutils.fs.rm(bronze_table_path, recurse=True)
    dbutils.fs.rm(silver_table_path, recurse=True)
    dbutils.fs.rm(gold_table_path, recurse=True)
    
    dataPath = f"{dbfs_data_path}historical_sensor_data.csv"
    
    df = spark.read\
      .option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv(dataPath)
    
    df.write \
      .format("delta").mode("overwrite").saveAsTable(f"{database}.sensor_readings_historical_bronze")
    
  
  bronze_sample_exists = spark._jsparkSession.catalog().tableExists(database, 'sensor_readings_historical_bronze_sample')
  
  if not bronze_sample_exists:
    df = spark.sql(f'select * from {database}.sensor_readings_historical_bronze').sample(False, 0.05, 42)
    df.write.format('delta').mode('overwrite').saveAsTable(f'{database}.sensor_readings_historical_bronze_sample')    
    
  out = {
    "database_name": database
  }
  
  return out

# COMMAND ----------

# create path if it does not exist
# dbutils.fs.mkdirs(base_table_path)

# download_datasets(local_data_path)

# COMMAND ----------

# # Return to the caller, passing the variables needed for file paths and database

# response = local_data_path + " " + base_table_path + " " + database_name

# dbutils.notebook.exit(response)
