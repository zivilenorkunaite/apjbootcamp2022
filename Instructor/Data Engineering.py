# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # AP Juice Lakehouse Platform
# MAGIC 
# MAGIC Data Sources:
# MAGIC - CRM
# MAGIC - Sales Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![ ](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC Let's load locations data from our CRM to Delta Lake. In our case we don't want to track any history and opt to overwrite data every time process is running.
# MAGIC 
# MAGIC CRM export is stored as CSV file

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- move this one to setup
# MAGIC CREATE DATABASE IF NOT EXISTS zivile_bootcamp;
# MAGIC USE zivile_bootcamp;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load Store Locations data to Delta Table
# MAGIC 
# MAGIC In our example CRM export has been provided to us as a CSV file. We know that we will not need to keep and manage history for this data so creating table can be a simple overwrite each time ETL runs.

# COMMAND ----------

dbfs_data_path = 'dbfs:/FileStore/zivile_norkunaite/'

dataPath = f"{dbfs_data_path}apj_locations.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data is in DataFrame, but not yet in Delta Table. We can use SQL to copy data to the table

# COMMAND ----------

# Creating a Temporary View will allow us to use SQL to interact with data
df.createOrReplaceTempView("bronze_apj_locations_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from bronze_apj_locations_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL DDL can be used to create table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS zivile_bootcamp.bronze_apj_locations;
# MAGIC 
# MAGIC CREATE TABLE zivile_bootcamp.bronze_apj_locations
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM bronze_apj_locations_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED zivile_bootcamp.bronze_apj_locations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now that we know the location of this table, let's look at the files

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/zivile_bootcamp.db/bronze_apj_locations')

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/zivile_bootcamp.db/bronze_apj_locations/_delta_log')

# COMMAND ----------

# format these better for display
dbutils.fs.head('dbfs:/user/hive/warehouse/zivile_bootcamp.db/bronze_apj_locations/_delta_log/00000000000000000000.json')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This is a MANAGED table - we can see it in DESCRIBE EXTENDED results as well as in the transaction log. That means dropping table will delete all data files as well - let's try it!
# MAGIC 
# MAGIC Another way to run SQL statements is by using spark.sql() command in python cell

# COMMAND ----------

spark.sql("DROP TABLE zivile_bootcamp.bronze_apj_locations");
dbutils.fs.ls('dbfs:/user/hive/warehouse/zivile_bootcamp.db/bronze_apj_locations');

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To create an UNMANAGED table we need to specify LOCATION in the CREATE TABLE statement

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS zivile_bootcamp.bronze_apj_locations;
# MAGIC 
# MAGIC CREATE TABLE zivile_bootcamp.bronze_apj_locations
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/zivile_norkunaite/unmanaged_bronze_apj_locations2'
# MAGIC AS
# MAGIC SELECT * FROM bronze_apj_locations_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE EXTENDED zivile_bootcamp.bronze_apj_locations;

# COMMAND ----------

spark.sql("DROP TABLE zivile_bootcamp.bronze_apj_locations");
dbutils.fs.ls('dbfs:/tmp/zivile_norkunaite/unmanaged_bronze_apj_locations');

# COMMAND ----------

dbutils.fs.ls('dbfs:/tmp/zivile_norkunaite/unmanaged_bronze_apj_locations2');

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create silver table

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS zivile_bootcamp.silver_apj_locations;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE zivile_bootcamp.silver_apj_locations AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   store_name,
# MAGIC   store_address,
# MAGIC   case
# MAGIC     when has_online_shop = 'Y' then True
# MAGIC     else False
# MAGIC   end as has_online_shope,
# MAGIC   location_timezone,
# MAGIC   open_hours,
# MAGIC   busy_hours,
# MAGIC   number_of_customers,
# MAGIC   missing_customers,
# MAGIC   coalesce(min_sales, 0) as min_sales,
# MAGIC   max_sales,
# MAGIC   case
# MAGIC     when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL'
# MAGIC     else 'AUS'
# MAGIC   end as store_country
# MAGIC FROM
# MAGIC   bronze_apj_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select store_country, count(1) as number_of_stores 
# MAGIC from zivile_bootcamp.silver_apj_locations
# MAGIC group by store_country

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Incremental changes 
# MAGIC 
# MAGIC # Autoloader

# COMMAND ----------

# load all files as batch, trigger once to bronze sales table 

# COMMAND ----------

# add incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- data test temp
# MAGIC 
# MAGIC -- silver table: could split by order + order items, easy way to get total cost of the order
# MAGIC with ct as (
# MAGIC select saleID, explode(from_json(saleitems,'array<struct<id:string,size:string,notes:string,cost:double,ingredients:array<string>>>')) ex from zivile_demo.apj_juice_sales
# MAGIC limit 20 )
# MAGIC 
# MAGIC select saleID, sum(ex.cost) as total_cost from ct group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select location, date_trunc("Hour",from_unixtime(ts)) as ts, sum(1) as sales 
# MAGIC from zivile_demo.apj_juice_sales
# MAGIC group by 2, 1 
# MAGIC order by 2, 1
