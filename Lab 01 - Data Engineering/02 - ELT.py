# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # AP Juice Lakehouse Platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC 
# MAGIC In this Notebook we will see how to implement Medalion Architecture on your Lakehouse. 
# MAGIC 
# MAGIC Some of the things we will look at are:
# MAGIC * Using Auto-loader
# MAGIC    * Batch and Stream Ingestion
# MAGIC    * Schema Evolution
# MAGIC * Optimizing tables for specific query pattern using OPTIMIZE and ZORDER
# MAGIC * Incremental updates using MERGE
# MAGIC * Scheduling jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ### APJ Data Sources
# MAGIC 
# MAGIC For this exercise we will be starting to implement Lakehouse platform for our company, AP Juice.
# MAGIC 
# MAGIC AP Juice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
# MAGIC 
# MAGIC For this part of the exercise we will be processing 3 existing dimensions and sales transactions datasets. Files will be a mix of `csv` and `json` files and our goal is to have **incremental updates** for sales table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using Databricks Notebooks workflow[https://docs.databricks.com/notebooks/notebook-workflows.html] element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system (check our `dbutils.fs.rm()` being used in the next cell) or to read Secrets.

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

bronze_table_path = f"{dbfs_data_path}tables/bronze/"
silver_table_path = f"{dbfs_data_path}tables/silver/"
gold_table_path = f"{dbfs_data_path}tables/gold/"

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

# MAGIC %md
# MAGIC 
# MAGIC We can also run another notebook via magic `%run` command.  When we use `%run`, the called notebook is immediately executed and the functions and variables defined in it become available in the calling notebook. On the other hand, the `dbutils.notebook.run()` used above starts a new job to run the notebook.
# MAGIC 
# MAGIC In this case we will use separate notebook to define few functions.

# COMMAND ----------

# MAGIC %run ./Utils/Define-Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![ ](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We already have seen store locations dataset. Let's redo the work this time using suggested Delta Architecture steps

# COMMAND ----------

data_file_location = f"{dbfs_data_path}/stores.csv"

bronze_table_name = "bronze_store_locations"
silver_table_name = "dim_locations"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(data_file_location)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .option("path", f"{bronze_table_path}/{bronze_table_name}") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
select *, 
case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code
from {bronze_table_name}
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

data_file_location = f"{dbfs_data_path}/users.csv"

bronze_table_name = "bronze_customers"
silver_table_name = "dim_customers"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(data_file_location)

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

data_file_location = f"{dbfs_data_path}/products.json"

bronze_table_name = "bronze_products"
silver_table_name = "dim_products"

df = spark.read\
  .json(data_file_location)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .option("path", f"{bronze_table_path}/{bronze_table_name}") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
select * from {bronze_table_name}
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
# MAGIC Easy way to bring incremental data to our Delta Lake is by using **autoloader**.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Prepare for the first autoloader run - as this is an example Notebook, we can delete all the files and tables before running it.

# COMMAND ----------

import pyspark.sql.functions as F

checkpoint_path = f'{local_data_path}/_checkpoints'
schema_path = f'{local_data_path}/_schema'
write_path = f'{bronze_table_path}/bronze_sales'

spark.sql("drop table if exists bronze_sales")

refresh_autoloader_datasets = True

if refresh_autoloader_datasets:
  # Run these only if you want to start a fresh run!
  dbutils.fs.rm(checkpoint_path,True)
  dbutils.fs.rm(schema_path,True)
  dbutils.fs.rm(write_path,True)
  dbutils.fs.rm(autoloader_ingest_path, True)
  
  dbutils.fs.mkdirs(autoloader_ingest_path)
  
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202110.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202111.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202112.json", autoloader_ingest_path)



# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df =spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 


batch_autoloader = df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .trigger(once=True) \
  .table('bronze_sales')

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
# MAGIC describe extended bronze_sales;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Schema Evolution
# MAGIC 
# MAGIC Let's simulate SYD01 location uploading a new data file by running a custom python function. After data is generated - run the same autoloader script again (make sure to NOT delete checkpoint files this time).
# MAGIC 
# MAGIC This newly generated dataset will have a new column `exported_ts` with a timestamp value of when data was exported from our source system.

# COMMAND ----------

get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-01') 

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 


batch_autoloader = df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .trigger(once=True) \
  .table('bronze_sales')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Why did the query in a cell above fail?
# MAGIC 
# MAGIC Over time data sources schema can change. In traditional ETL that would mean changing the scripts and loosing all new data up before the change is executed.
# MAGIC 
# MAGIC Autoloader can automatically pick up new columns - run the cell above again and check what is the bronze_sales table columns are like now.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select file_path, count(*) number_of_records
# MAGIC from bronze_sales
# MAGIC group by file_path;

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
# MAGIC Start the autoloader running cell bellow, wait for stream to start and generate new upload file by running `get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-02')`. You can see new files being processed as they are uploaded.

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df =spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 


streaming_autoloader = df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .table('bronze_sales')

# COMMAND ----------

get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-02')

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
# MAGIC Now that we have a bronze table ready - let's a create silver one! 
# MAGIC 
# MAGIC We can start by using same approach as for the dimension tables earlier - clean and de-duplicate data from bronze table, rename columns to be more business friendly and save it as silver table.

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
# MAGIC   select
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

spark.sql("drop table if exists silver_sales;")

spark.sql(f"""
create table silver_sales 
using delta
location '{bronze_table_path}silver_sales'
as
select * from v_silver_sales;
""")

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
# MAGIC all_records as (
# MAGIC   select
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
# MAGIC   all_records

# COMMAND ----------

spark.sql("drop table if exists silver_sale_items");

spark.sql(f"""
create table silver_sale_items
using delta
location '{bronze_table_path}silver_sale_items'
as
select * from v_silver_sale_items;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### OPTIMIZE
# MAGIC 
# MAGIC 
# MAGIC Run a query to find a specific order in `silver_sale_items` table and note query execution time. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sale_items 
# MAGIC where sale_id = '00139294-b5c5-4af1-9b4c-181c1911ad16';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we know that most of the queries will be using `sale_id` filter - we can optimize this table by running `ZORDER` on that column.
# MAGIC 
# MAGIC Running `OPTIMIZE` on a table on Delta Lake on Databricks can improve the speed of read queries from a table by coalescing small files into larger ones. 
# MAGIC 
# MAGIC Default output file size is 1GB, but in our relatively small dataset it would be better to have smaller files.

# COMMAND ----------

# set max file size to 50MB
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 52428800)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize silver_sale_items
# MAGIC zorder by sale_id

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sale_items 
# MAGIC where sale_id = '002be97c-e70c-4ddd-9c5d-e9cce8bb5771';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC How did this `OPTIMIZE` command help? It is all in Delta Log files!

# COMMAND ----------

dbutils.fs.ls(f"{bronze_table_path}silver_sale_items/_delta_log/")

# COMMAND ----------

spark.sql(f"select add.path as filename, add.stats:minValues, add.stats:maxValues from json.`{bronze_table_path}silver_sale_items/_delta_log/00000000000000000001.json` where add is not null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MERGE
# MAGIC 
# MAGIC For a given day store sent us records twice - second time was to close all pending sales.
# MAGIC 
# MAGIC Make sure your autoloader is still running in streaming mode (or start it again) to process these new records.

# COMMAND ----------

get_fixed_records_data(autoloader_ingest_path, 'SYD01','2022-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronze_sales order by inserted_at desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC `_resqued_data` column contains any parsing errors. There should be none if everything remains as an autoloader default string, but we have provided SchemaHints value before.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update bronze_sales
# MAGIC set ts = unix_timestamp(_rescued_data:ts)
# MAGIC where _rescued_data is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronze_sales
# MAGIC where location = 'SYD01'
# MAGIC and saleid = 'd2e70607-02f7-417d-a5cb-be301c66bb03'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sales
# MAGIC where store_id = 'SYD01'
# MAGIC and id = 'd2e70607-02f7-417d-a5cb-be301c66bb03'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC merge into silver_sales target
# MAGIC    using v_silver_sales source
# MAGIC    on target.id = source.id
# MAGIC when matched and target.row_hash <> source.row_hash then 
# MAGIC   update set *
# MAGIC when not matched then
# MAGIC   insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sales
# MAGIC where store_id = 'SYD01'
# MAGIC and id = 'd2e70607-02f7-417d-a5cb-be301c66bb03'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists gold_country_sales;
# MAGIC 
# MAGIC create table gold_country_sales 
# MAGIC as 
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
# MAGIC create table gold_top_customers 
# MAGIC as
# MAGIC select s.store_id, ss.unique_customer_id, c.name, sum(product_cost) total_spend 
# MAGIC from silver_sale_items s 
# MAGIC   join silver_sales ss on s.sale_id = ss.id
# MAGIC   join dim_customers c on ss.unique_customer_id = c.unique_id
# MAGIC where ss.unique_customer_id is not null 
# MAGIC group by s.store_id, ss.unique_customer_id, c.name;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from gold_top_customers

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Stop streaming autoloader to allow our cluster to shut down.

# COMMAND ----------

streaming_autoloader.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Scheduled Updates

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can schedule this Notebook to run every day.
