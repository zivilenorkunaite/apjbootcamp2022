# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # APJuice Lakehouse Platform
# MAGIC 
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC In this Notebook we will see how to work with Delta Tables when using Databricks Notebooks. 
# MAGIC 
# MAGIC Some of the things we will look at are:
# MAGIC * Creating a new Delta Table
# MAGIC * Data transformations like Merge, upserts, delete
# MAGIC * Using Delta Log and Time Traveling 
# MAGIC * Tracking data changes using Change Data Feed
# MAGIC * Cloning tables
# MAGIC * Masking data by using Dynamic Views
# MAGIC 
# MAGIC In addition to Delta Tables we will also get to see some tips and tricks on working on Databricks environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ### APJ Data Sources
# MAGIC 
# MAGIC For this exercise we will be starting to implement Lakehouse platform for our company, APJuice.
# MAGIC 
# MAGIC APJuice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
# MAGIC 
# MAGIC For the first part of the exercise we will be focusing on an export of Store Locations table that has been saved as `csv` file.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using [Databricks Notebooks workflow](https://docs.databricks.com/notebooks/notebook-workflows.html) element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system or reading [Databricks Secrets](https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-secrets)

# COMMAND ----------

# MAGIC %run "../Lab 01 - Data Engineering/Utils/prepare-lab-environment"

# COMMAND ----------

generate_sales_dataset()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC Let's load store locations data to Delta Table. In our case we don't want to track any history and opt to overwrite data every time process is running.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Delta Table
# MAGIC 
# MAGIC ***Load Store Locations data to Delta Table***
# MAGIC 
# MAGIC In our example CRM export has been provided to us as a CSV file and uploaded to `dbfs_data_path` location. It could also be your S3 bucket, Azure Storage account or Google Cloud Storage. 
# MAGIC 
# MAGIC We will not be looking at how to set up access to files on the cloud environment in today's workshop.
# MAGIC 
# MAGIC 
# MAGIC For our APJ Data Platform we know that we will not need to keep and manage history for this data so creating table can be a simple overwrite each time ETL runs.
# MAGIC 
# MAGIC 
# MAGIC Let's start with simply reading CSV file into DataFrame

# COMMAND ----------

dataPath = f"file:{git_datasets_location}stores.csv"

df = spark.read\
    .option("header","true")\
    .option("inferSchema", "true")\
    .csv(dataPath)
    

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data is in a DataFrame, but not yet in a Delta Table. Still, we can already use SQL to query data or copy it into the Delta table

# COMMAND ----------

# Creating a Temporary View will allow us to use SQL to interact with data

df.createOrReplaceTempView("stores_csv_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from stores_csv_file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL DDL can be used to create table using view we have just created. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS stores;
# MAGIC 
# MAGIC CREATE TABLE stores
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM stores_csv_file;
# MAGIC 
# MAGIC SELECT * from stores;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This SQL query has created a simple Delta Table (as specified by `USING DELTA`). DELTA a default format so it would create a delta table even if we skip the `USING DELTA` part.
# MAGIC 
# MAGIC For more complex tables you can also specify table PARTITION or add COMMENTS.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lab trial : create a delta table with the products.json in the datasets
# MAGIC 
# MAGIC You can use this [page](https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/api/pyspark.sql.DataFrameReader.json.html?highlight=json&_gl=1*i72q5f*_gcl_aw*R0NMLjE2NzkyODA1ODkuQ2p3S0NBanc1ZHFnQmhCTkVpd0E3UHJ5YUFpNVFQMUxMWDhIY3ZfRm9KdUtDZzdrU1V3QmxhcHRjSG1jZU9SUHN4cm5xYXRUS2xvU1ZCb0MyX1lRQXZEX0J3RQ..&_ga=2.83797270.1008137546.1679267788-930940010.1670205054#pyspark.sql.DataFrameReader.json) for example. Further if you want to write a dataframe without creating a view use this [page](https://docs.databricks.com/delta/tutorial.html#create-a-table) for reference

# COMMAND ----------

filepath = f"file:{git_datasets_location}products.json"
products_df = spark.read.json(filepath)
display(products_df)

# COMMAND ----------

products_df.write.saveAsTable("products")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe Delta Table
# MAGIC 
# MAGIC Now that we have created our first Delta Table - let's see what it looks like on our database and where are the data files stored.  
# MAGIC 
# MAGIC Quick way to get information on your table is to run `DESCRIBE EXTENDED` command on SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED stores

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe HISTORY stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC One of the rows in output above provides us with `Location` information. This is where the actual delta files are being stored.
# MAGIC 
# MAGIC Lets observe how we can do various transformations steps on a Delta Table 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Update Delta Table
# MAGIC 
# MAGIC Provided dataset has address information, but no country name - let's add one!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC alter table stores
# MAGIC add column store_country string;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update stores
# MAGIC set store_country = case when id in ('SYD01', 'MEL01', 'BNE02','CBR01','PER01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end

# COMMAND ----------

# MAGIC %sql 
# MAGIC select store_country, id, name from stores

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###LAB EXERCISE : Missed store_country for id MEL02. Update the store_country as AUS for id MEL02

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update stores
# MAGIC set store_country = 'AUS'
# MAGIC where id = 'MEL02'

# COMMAND ----------

# MAGIC %sql
# MAGIC select store_country, count(id) as number_of_stores from stores group by store_country

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table stores

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Merge Delta Table
# MAGIC 
# MAGIC Upsert changes into Delta table. 

# COMMAND ----------

# DBTITLE 1,Lets create some new stores
# MAGIC 
# MAGIC %sql
# MAGIC create table if not exists update_stores (id STRING, name STRING, email STRING, city STRING, hq_address STRING, phone_number STRING, store_country STRING);
# MAGIC delete from update_stores;
# MAGIC insert into update_stores values('ADL01', 'Adelaide CBD', 'janedane@apjuice.au', 'Adelaide','25 Pirie Street Adelaide SA 5000', '0732975665', 'AUS');
# MAGIC insert into update_stores values('ADL02', 'Adelaide Airport', 'janedane@apjuice.au', 'Adelaide','Sir Richard Williams Ave, Adelaide Airport SA 5950', '34652308', 'AUS');
# MAGIC insert into update_stores values('AKL02', 'Auckland CBD', 'jeremysmith@apjuice.au', 'Auckland' ,'85 Webb Street MacDonaldmouth 5504', '+64273484326', 'NZL')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO stores
# MAGIC USING update_stores
# MAGIC ON update_stores.id = stores.id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT * ;
# MAGIC 
# MAGIC select * from stores;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###ALTER TABLE
# MAGIC 
# MAGIC To modify the schema or properties of a table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE stores SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5');

# COMMAND ----------

# DBTITLE 1,Change column name
# MAGIC %sql
# MAGIC ALTER TABLE stores ALTER COLUMN id COMMENT "unique id for the store" ;
# MAGIC ALTER TABLE stores RENAME COLUMN id TO store_id;
# MAGIC DESCRIBE TABLE stores;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Table Constraints
# MAGIC 
# MAGIC Constraints ensure Data quality and integrity on the Delta Tables. You can use the same **ALTER TABLE** to add constraints

# COMMAND ----------

# MAGIC %md
# MAGIC ###LAB Exercise : Add a constraint to the table for **id** to not be NULL. Refer this [page](https://docs.databricks.com/tables/constraints.html#set-a-not-null-constraint-in-databricks) for syntax help
# MAGIC Verify by inserting a value into the table with a NULL value for ___store_id___

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta History and Time Travel
# MAGIC 
# MAGIC 
# MAGIC Delta Tables keep all changes made in the delta log we've seen before. There are multiple ways to see that - e.g. by running `DESCRIBE HISTORY` for a table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Having all this information and old data files mean that we can **Time Travel**!  You can query your table at any given `VERSION AS OF` or  `TIMESTAMP AS OF`.
# MAGIC 
# MAGIC Let's check again what table looked like before we ran last update

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from stores VERSION AS OF 2 where id = 'MEL02';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO stores values ('SYD02', 'Sydney Bondi', 'joe@apjuic.au','Sydney', '66 King Street, 2000', '046562498','AUS')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Lab exercise : Perform an anti join for the same table with previous version ###
# MAGIC Refer to the [Join](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-join.html) syntax for further details

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Change Data Feed
# MAGIC 
# MAGIC 
# MAGIC The Delta change data feed represents row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records “change events” for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.
# MAGIC 
# MAGIC It is not enabled by default, but we can enabled it using `TBLPROPERTIES`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE stores SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Changes to table properties also generate a new version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change data feed can be seen by using `table_changes` function. You will need to specify a range of changes to be returned - it can be done by providing either version or timestamp for the start and end. The start and end versions and timestamps are inclusive in the queries. 
# MAGIC 
# MAGIC To read the changes from a particular start version to the latest version of the table, specify only the starting version or timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- simulate change of address for store AKL01 and removal of store BNE02
# MAGIC 
# MAGIC update stores
# MAGIC set hq_address = 'Domestic Terminal, AKL'
# MAGIC where store_id = 'AKL01';
# MAGIC 
# MAGIC delete from stores
# MAGIC where store_id = 'BNE02';
# MAGIC 
# MAGIC SELECT * FROM table_changes('stores', 11, 13) -- Note that we increment versions due to UPDATE statements above

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Delta CDC gives back 4 cdc types in the "__change_type" column:
# MAGIC 
# MAGIC | CDC Type             | Description                                                               |
# MAGIC |----------------------|---------------------------------------------------------------------------|
# MAGIC | **update_preimage**  | Content of the row before an update                                       |
# MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
# MAGIC | **delete**           | Content of a row that has been deleted                                    |
# MAGIC | **insert**           | Content of a new row that has been inserted                               |
# MAGIC 
# MAGIC Therefore, 1 update results in 2 rows in the cdc stream (one row with the previous values, one with the new values)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lab Exercise:  Execute CDC changes for the same delta table but with timestamp version ###

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CLONE
# MAGIC 
# MAGIC 
# MAGIC What if our use case is more of a having monthly snapshots of the data instead of detailed changes log? Easy way to get it done is to create CLONE of table.
# MAGIC 
# MAGIC You can create a copy of an existing Delta table at a specific version using the clone command. Clones can be either deep or shallow.
# MAGIC 
# MAGIC  
# MAGIC 
# MAGIC * A **deep clone** is a copy of all the underlying files of the source table data in addition to the metadata of the existing table. Deep clones are useful for testing in a production environment, data migration and staging major changes to a production table
# MAGIC * A **shallow clone** is a clone that does not copy the data files to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create, but they will break if original data files were not available. Beneficial for short-lived use cases such as testing or any experimentation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists stores_clone;
# MAGIC 
# MAGIC create table stores_clone DEEP CLONE stores VERSION AS OF 3 -- you can specify timestamp here instead of a version

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history stores_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists stores_clone_shallow;
# MAGIC 
# MAGIC -- Note that no files are copied
# MAGIC 
# MAGIC create table stores_clone_shallow SHALLOW CLONE stores
