# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # AP Juice Lakehouse Platform
# MAGIC 
# MAGIC 
# MAGIC ADD LOGO

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC Add agenda

# COMMAND ----------

# MAGIC %md
# MAGIC ### APJ Data Sources
# MAGIC 
# MAGIC Data Sources:
# MAGIC - CRM
# MAGIC - Sales Data
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Describe data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using Databricks Notebooks workflow[https://docs.databricks.com/notebooks/notebook-workflows.html] element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system (check our `dbutils.fs.rm()` being used in the next cell) or to read Secrets[ ADD LINK ]
# MAGIC 
# MAGIC We can also call notebook by using `%run` magic command

# COMMAND ----------

import random 

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0).split()


local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

bronze_table_path = f"{dbfs_data_path}tables/bronze"
silver_table_path = f"{dbfs_data_path}tables/silver"
gold_table_path = f"{dbfs_data_path}tables/gold"

autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"

# Remove all files from location in case there were any
dbutils.fs.rm(bronze_table_path, recurse=True)
dbutils.fs.rm(silver_table_path, recurse=True)
dbutils.fs.rm(gold_table_path, recurse=True)

print("Local data path is {}".format(local_data_path))
print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))

print("Brone Table Location is {}".format(bronze_table_path))
print("Silver Table Location is {}".format(silver_table_path))
print("Gold Table Location is {}".format(gold_table_path))

spark.sql(f"USE {database_name};")

# COMMAND ----------

# TEMPORARY - REPLACE PATH FOR NOW, NEEDS TO BE MOVED TO GDRIVE AND ADDED AS PART OF SETUP SCRIPT

dbfs_data_path = '/mnt/apjbootcamp/DATASETS'
autoloader_ingest_path = "/mnt/apjbootcamp/autoloader/"

refresh_autoloader = True

if refresh_autoloader:
  dbutils.fs.rm(autoloader_ingest_path, True)
  dbutils.fs.mkdirs(autoloader_ingest_path)
  
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202110.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202111.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202112.json", autoloader_ingest_path)


def get_incremental_data(location, date):
    df = spark.sql(f"""
  select CustomerID, Location, OrderSource, PaymentMethod, STATE, SaleID, SaleItems, ts, unix_timestamp() as exported_ts from apjbootcamp_working_db.jan_sales
where location = '{location}' and ts_date = '{date}'
  """)
    df \
    .coalesce(1) \
    .write \
    .mode('overwrite') \
    .json(f"{autoloader_ingest_path}{location}/{date}/daily_sales.json")
 
  
def get_fixed_records_data(location, date):
  df = spark.sql(f"""
  select CustomerID, Location, OrderSource, PaymentMethod, 'CANCELED' as STATE, SaleID, SaleItems, from_unixtime(ts) as ts, unix_timestamp() as exported_ts from apjbootcamp_working_db.jan_sales
where location = '{location}' and ts_date = '{date}'
and state = 'PENDING'
  """)
  df \
  .coalesce(1) \
  .write \
  .mode('overwrite') \
  .json(f"{autoloader_ingest_path}{location}/{date}/updated_daily_sales.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![ ](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We already have seen store locations dataset. Let's redo the work this time using suggested Delta Architecture steps

# COMMAND ----------

dbutils.fs.ls(dbfs_data_path)

# COMMAND ----------

dataPath = f"{dbfs_data_path}/stores.csv"

bronze_table_name = "bronze_store_locations"
silver_table_name = "dim_locations"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .option("path", f"{bronze_table_path}/{bronze_table_name}") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
SELECT *, case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code
FROM {bronze_table_name}
""")

spark.sql(f"DROP TABLE IF EXISTS {silver_table_name};")


silver_df.write \
  .mode("overwrite") \
  .option("path", f"{silver_table_path}/{silver_table_name}") \
  .saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We have 2 more dimension tables that can be added to the Lakehouse without many data changes - dim_customers and dim_products.

# COMMAND ----------

dataPath = f"{dbfs_data_path}/users.csv"

bronze_table_name = "bronze_customers"
silver_table_name = "dim_customers"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .option("path", f"{bronze_table_path}/{bronze_table_name}") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
SELECT store_id || "-" || cast(id as string) as unique_id, id, store_id, name, email FROM {bronze_table_name}
""")

spark.sql(f"DROP TABLE IF EXISTS {silver_table_name};")


silver_df.write \
  .mode("overwrite") \
  .option("path", f"{silver_table_path}/{silver_table_name}") \
  .saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC And repeat for dim_products - note that this time our input file is json and not csv

# COMMAND ----------

dataPath = f"{dbfs_data_path}/products.json"

bronze_table_name = "bronze_products"
silver_table_name = "dim_products"

df = spark.read\
  .json(dataPath)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .option("path", f"{bronze_table_path}/{bronze_table_name}") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
SELECT * FROM {bronze_table_name}
""")

spark.sql(f"DROP TABLE IF EXISTS {silver_table_name};")


silver_df.write \
  .mode("overwrite") \
  .option("path", f"{silver_table_path}/{silver_table_name}") \
  .saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Autoloader
# MAGIC 
# MAGIC In hour example most of the locations upload their sales data to our storage location on nightly batches. Some of them however have upgraded to hourly or even more frequent data feeds.
# MAGIC 
# MAGIC Easy way to bring incremental data to our Delta Lake is by using **autoloader**.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Prepare for first autoloader run

# COMMAND ----------

import pyspark.sql.functions as F

checkpoint_path = f'{local_data_path}/_checkpoints'
schema_path = f'{local_data_path}/_schema'
write_path = f'{bronze_table_path}/bronze_sales'

spark.sql("DROP TABLE IF EXISTS bronze_sales")

# Run these only if you want to start a fresh run!
dbutils.fs.rm(checkpoint_path,True)
dbutils.fs.rm(schema_path,True)
dbutils.fs.rm(write_path,True)


# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df =spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 


df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .trigger(once=True) \
  .table('bronze_sales')

# COMMAND ----------

# TODO - change code above to write to path and create table with wait till files are uploaded loop

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Wait for the Autoloader Stream to finish and check how many records got inserted - calculated column `file_path` is a good way to see it

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select file_path, count(*) number_of_records
# MAGIC from bronze_sales
# MAGIC group by file_path;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  * from bronze_sales limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED bronze_sales;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Let's simulate SYD01 location uploading a new data file by running a provided python function. After data is generated - run the same autoloader script again (make sure to NOT delete checkpoint files this time).

# COMMAND ----------

get_incremental_data('SYD01','2022-01-01')  # this will have a new column exported_ts with timestamp of when data was exported from source system

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df =spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 


df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .trigger(once=True) \
  .table('bronze_sales')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Schema Evolution
# MAGIC 
# MAGIC Why did the query in a cell above fail?
# MAGIC 
# MAGIC Over time data sources schema can change. In traditional ETL that would mean changing the scripts and loosing all new data up before the change is executed.
# MAGIC 
# MAGIC Autoloader can automatically pick up new columns - run the cell above again and check what is the bronze_sales table columns are like now.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronze_sales order by inserted_at desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can schedule autoloder to run on required schedule (e.g. every night) and it will always process files uploaded since last run.
# MAGIC 
# MAGIC What if we would like to process files as soon as they are uploaded? Autoloader can run in **streaming mode** with one simple change in the code used - removing Trigger Once option.
# MAGIC 
# MAGIC Start the autoloader running cell bellow, wait for stream to start and generate new upload file by running `get_incremental_data('SYD01','2022-01-02')`. You can see new files being processed as they are uploaded.

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df =spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 


df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .table('bronze_sales')

# COMMAND ----------

get_incremental_data('SYD01','2022-01-02')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this to see newly processed rows
# MAGIC select file_path, count(*) number_of_records
# MAGIC from bronze_sales
# MAGIC group by file_path

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Silver Tables
# MAGIC 
# MAGIC Now that we have a bronze table ready - let's a create silver one!  We can start by using same approach as for the dimension tables earlier - clean and de-duplicate data from bronze table and save it as silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create or replace view v_silver_sales 
# MAGIC as 
# MAGIC with with_latest_record_id as (
# MAGIC   select
# MAGIC     *,
# MAGIC     row_number() over (
# MAGIC       partition by SaleID
# MAGIC       order by
# MAGIC         coalesce(exported_ts, 0) desc
# MAGIC     ) as latest_record
# MAGIC   from
# MAGIC     bronze_sales
# MAGIC ),
# MAGIC newest_records as (
# MAGIC   SELECT
# MAGIC     saleID as id,
# MAGIC     from_unixtime(ts) as ts,
# MAGIC     Location as store_id,
# MAGIC     CustomerID as customer_id,
# MAGIC     location || "-" || cast(CustomerID as string) as unique_customer_id,
# MAGIC     OrderSource as order_source,
# MAGIC     STATE as order_state,
# MAGIC     SaleItems as sale_items
# MAGIC   from
# MAGIC     with_latest_record_id
# MAGIC   where
# MAGIC     latest_record = 1
# MAGIC )
# MAGIC select
# MAGIC   *,
# MAGIC   sha2(concat_ws(*, '||'), 256) as row_hash -- add a hash of all values to easily pick up changed rows
# MAGIC from
# MAGIC   newest_records

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC drop table if exists silver_sales;
# MAGIC 
# MAGIC create table silver_sales 
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (store_id) 
# MAGIC AS
# MAGIC SELECT * FROM v_silver_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Sales table is nice, but we also have sales items information object that can be split into rows for easier querying

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace view v_silver_sale_items 
# MAGIC as 
# MAGIC 
# MAGIC with itemised_records as (
# MAGIC   select
# MAGIC     *,
# MAGIC     posexplode(
# MAGIC       from_json(
# MAGIC         sale_items,
# MAGIC         'array<struct<id:string,size:string,notes:string,cost:double,ingredients:array<string>>>'
# MAGIC       )
# MAGIC     )
# MAGIC   from
# MAGIC     v_silver_sales
# MAGIC ),
# MAGIC newest_records as (
# MAGIC   SELECT
# MAGIC     id || "-" || cast(pos as string) as id,
# MAGIC     id as sale_id,
# MAGIC     store_id,
# MAGIC     pos as item_number,
# MAGIC     col.id as product_id,
# MAGIC     col.size as product_size,
# MAGIC     col.notes as product_notes,
# MAGIC     col.cost as product_cost,
# MAGIC     col.ingredients as product_ingredients
# MAGIC   from
# MAGIC     itemised_records
# MAGIC )
# MAGIC select
# MAGIC   *,
# MAGIC   sha2(concat_ws(*, '||'), 256) as row_hash
# MAGIC from
# MAGIC   newest_records

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver_sale_items;
# MAGIC 
# MAGIC create table silver_sale_items
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (store_id)
# MAGIC AS
# MAGIC SELECT * from v_silver_sale_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sale_items limit 5;

# COMMAND ----------

# MAGIC %md 
# MAGIC MOVE OPTIMIZE PART HERE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This is great for one-off run, but what if we want to keep updating table every day with new data only?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MERGE
# MAGIC 
# MAGIC For a given day store sent us records twice - second time was to close all pending sales.
# MAGIC 
# MAGIC Make sure your autoloader is still running in streaming mode (or start ir again) to process these new records.

# COMMAND ----------

get_fixed_records_data('SYD01','2022-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronze_sales order by inserted_at desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC TODO : _resqued_data column

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update bronze_sales
# MAGIC set ts = unix_timestamp(from_json( _rescued_data,'struct<ts:timestamp,_file_path:string>').ts)
# MAGIC where _rescued_data is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronze_sales
# MAGIC where location = 'SYD01'
# MAGIC and saleid = '07c12e9b-a023-4883-8a14-fe4b8c62b147'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sales
# MAGIC where store_id = 'SYD01'
# MAGIC and id = '07c12e9b-a023-4883-8a14-fe4b8c62b147'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_sales target
# MAGIC    USING v_silver_sales source
# MAGIC    ON target.id = source.id
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_sale_items target
# MAGIC    USING v_silver_sale_items source
# MAGIC    ON target.id = source.id
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sales
# MAGIC where store_id = 'SYD01'
# MAGIC and id = '07c12e9b-a023-4883-8a14-fe4b8c62b147'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### OPTIMIZE

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/zivile_norkunaite_ap_juice_db.db/silver_sales/store_id=AKL01/')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history silver_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver_sales
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace view v_gold_country_sales as
# MAGIC 
# MAGIC select l.country_code, sum(product_cost) as total_sales, count(distinct sale_id) as number_of_sales
# MAGIC from silver_sale_items s join dim_locations l on s.store_id = l.id
# MAGIC group by l.country_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists gold_country_sales;
# MAGIC 
# MAGIC create table gold_country_sales as select * from v_gold_country_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace view v_gold_top_customers as 
# MAGIC 
# MAGIC select s.store_id, ss.unique_customer_id, c.name, sum(product_cost) total_spend 
# MAGIC from silver_sale_items s 
# MAGIC   join silver_sales ss on s.sale_id = ss.id
# MAGIC   join dim_customers c on ss.unique_customer_id = c.unique_id
# MAGIC where ss.unique_customer_id is not null 
# MAGIC group by s.store_id, ss.unique_customer_id, c.name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists gold_top_customers;
# MAGIC 
# MAGIC create table gold_top_customers as select * from v_gold_top_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Scheduled Updates

# COMMAND ----------

# more info on Jobs and how to create one
