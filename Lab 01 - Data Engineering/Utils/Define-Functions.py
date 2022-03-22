# Databricks notebook source
# MAGIC %run ./Fetch-User-Metadata

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}_aux")

spark.read.json(f"{base_table_path}sales_202201.json").createOrReplaceTempView('jan_sales_view')

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_name}_aux.jan_sales
AS 
SELECT *, from_unixtime(ts, "yyyy-MM-dd") as ts_date 
FROM jan_sales_view
ORDER BY from_unixtime(ts, "yyyy-MM-dd")
""")

# COMMAND ----------

import time

def get_incremental_data(ingest_path, location, date):
    df = spark.sql(f"""
  select CustomerID, Location, OrderSource, PaymentMethod, STATE, SaleID, SaleItems, ts, unix_timestamp() as exported_ts from {database_name}_aux.jan_sales
where location = '{location}' and ts_date = '{date}'
  """)
    df \
    .coalesce(1) \
    .write \
    .mode('overwrite') \
    .json(f"{ingest_path}{location}/{date}/daily_sales.json")
    time.sleep(5)
 
  
def get_fixed_records_data(ingest_path, location, date):
  df = spark.sql(f"""
  select CustomerID, Location, OrderSource, PaymentMethod, 'CANCELED' as STATE, SaleID, SaleItems, from_unixtime(ts) as ts, unix_timestamp() as exported_ts from {database_name}_aux.jan_sales
where location = '{location}' and ts_date = '{date}'
and state = 'PENDING'
  """)
  df \
  .coalesce(1) \
  .write \
  .mode('overwrite') \
  .json(f"{ingest_path}{location}/{date}/updated_daily_sales.json")
  time.sleep(2.4)
