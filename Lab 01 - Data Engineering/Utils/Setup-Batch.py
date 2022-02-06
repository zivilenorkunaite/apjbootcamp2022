# Databricks notebook source
# MAGIC %run ./Fetch-User-Metadata

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")

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
    "stores.csv": "10_6KJ8ve9bRSjThgVhUCzLsOvmCEFonL",
    "stores.json": "1vN7-zPDAdUddjX7e_forMzLCARnibnZl",
    "users.json": "13-5rhDQcgJEm86sxWajCnznFe57ITQXS",
    "users.csv": "1OAZkF_8iYl3_dWCBEhujDp-lZQUf7mul",
    "products.json": "1pFGmJgnteW52bK_9_SulP6UW9zHJIoDf",
    "sales_202110.json": "1DuPnbnVrqUzq1yXvLHiY7aClmJweMk5U",
    "sales_202111.json": "12A_GoQUje8fhLm_3quADaTHpVGyi8tI4",
    "sales_202112.json": "1x5HzV_SchNL6AYDf15ZNf8DxMVjJLuXI",
    "sales_202201.json": "195Nh-LvXNrsxwEtHd6hjNsPpIBRBN59d"
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

# create path if it does not exist
dbutils.fs.mkdirs(base_table_path)

download_datasets(local_data_path)

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)
