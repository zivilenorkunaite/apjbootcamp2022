# Databricks notebook source
# MAGIC %run ./Fetch-User-Metadata

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

def get_datasets_from_git(datasets_data_path, full_refresh=False):
  import os
  import zipfile

  dbutils.fs.mkdirs(datasets_data_path)
  
  working_dir = os.path.split(os.path.split(os.getcwd())[0])[0]
  with zipfile.ZipFile(f"{working_dir}/Datasets/sales2021.zip","r") as zip_ref:
      zip_ref.extractall(datasets_data_path)
  with zipfile.ZipFile(f"{working_dir}/Datasets/sales2022.zip","r") as zip_ref:
      zip_ref.extractall(datasets_data_path)
  with zipfile.ZipFile(f"{working_dir}/Datasets/dimensions.zip","r") as zip_ref:
      zip_ref.extractall(datasets_data_path)
      


# COMMAND ----------

# get datasets
try:
  get_datasets_from_git(local_data_path)
except Exception as e:
  print(e)
  !pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib tqdm
  
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
          if content_disposition is None:
            print("ERROR: GDrive download error. Wait few minutes and try again.")
          # parse filename
          filename = re.findall("filename=\"(.+)\"", content_disposition)[0]
          print("[+] File size:", file_size)
          print("[+] File name:", filename)
          progress = tqdm(response.iter_content(CHUNK_SIZE), \
                          f"Downloading {filename} to {destination}", \
                          total=file_size, unit="Byte", \
                          unit_scale=True, unit_divisor=1024)
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
      else:
        print(f'No token found for {id}')
        response = session.get(URL, params = {'id': id, 'confirm': 't'}, stream=True)
        #action="https://docs.google.com/uc?export=download&id=1DuPnbnVrqUzq1yXvLHiY7aClmJweMk5U&confirm=t"
        token = get_confirm_token(response)
        print(f'Try with new token: {token}')
        save_response_content(response, destination)
  
  
  def download_datasets_from_gdrive(datasets_data_path, full_refresh=False):

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
      dbutils.fs.rm(datasets_data_path.replace('/dbfs/','dbfs:/'), True)

    dbutils.fs.mkdirs(datasets_data_path.replace('/dbfs/','dbfs:/'))

    existing_datasets = [f.name for f in dbutils.fs.ls(datasets_data_path.replace('/dbfs/','dbfs:/'))]
    print(existing_datasets)

    # for each file check if it already exists and only download if not. Check by filename only
    for dataset_name in all_datasets_with_file_id:
      if not (dataset_name in existing_datasets):
        download_file_from_google_drive(all_datasets_with_file_id[dataset_name], f"{datasets_data_path}{dataset_name}")

  
  download_datasets_from_gdrive(local_data_path)

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)

# COMMAND ----------

#dbutils.fs.rm(local_data_path.replace('/dbfs/','dbfs:/'), True)
dbutils.fs.ls(local_data_path.replace('/dbfs/','dbfs:/'))
