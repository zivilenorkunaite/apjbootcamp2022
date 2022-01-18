# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # AP Juice Lakehouse Platform
# MAGIC 
# MAGIC Data Sources:
# MAGIC - CRM
# MAGIC - Sales Data
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Describe data
# MAGIC 
# MAGIC 
# MAGIC Add agenda

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Setup
# MAGIC 
# MAGIC We will be using Databricks Notebooks workflow[https://docs.databricks.com/notebooks/notebook-workflows.html] element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system (check our `dbutils.fs.rm()` being used in the next cell) or to read Secrets[]

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
bronze_table_path = f"{dbfs_data_path}tables/bronze"
silver_table_path = f"{dbfs_data_path}tables/silver"
gold_table_path = f"{dbfs_data_path}tables/gold"

apj_locations_data_path = f"{local_data_path}tables/apj_locations/"

autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"

# Remove all files from location in case there were any
dbutils.fs.rm(bronze_table_path, recurse=True)
dbutils.fs.rm(silver_table_path, recurse=True)
dbutils.fs.rm(gold_table_path, recurse=True)
dbutils.fs.rm(apj_locations_data_path, recurse=True)

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
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC Let's load locations data from our CRM to Delta Lake. In our case we don't want to track any history and opt to overwrite data every time process is running.
# MAGIC 
# MAGIC 
# MAGIC # ADD MORE ABOUT DELTA TABLES STRUCTURE HERE

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
# MAGIC Data is in a DataFrame, but not yet in a Delta Table. Still, we can already use SQL to query data or copy it into the Delta table

# COMMAND ----------

# MAGIC %python
# MAGIC # Creating a Temporary View will allow us to use SQL to interact with data
# MAGIC 
# MAGIC df.createOrReplaceTempView("apj_locations_csv_file")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can run SQL queries in the same notebook by using `%sql` magic command to change language just for that cell. You can also change language for all cells in this notebook by using default language setting!
# MAGIC 
# MAGIC # ADD IMAGES ON HOW TO CHANGE LANGUAGE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from apj_locations_csv_file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL DDL can be used to create table using existing view. 
# MAGIC 
# MAGIC There are some columns that do not make much sense for us now, but let's keep all the data for Bronze table - it is a good habit to have all data in Bronze tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS apj_locations;
# MAGIC 
# MAGIC CREATE TABLE apj_locations
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM apj_locations_csv_file;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This SQL query has created a simple Delta Table (as specified by `USING DELTA`). DELTA is actually a default format so it would create a delta table even if we skip the `USING DELTA` part.
# MAGIC 
# MAGIC For more complex tables you can also specify table PARTITION or add COMMENTS.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Describe Delta Table
# MAGIC 
# MAGIC Now that we have created our first Delta Table - let's see what it looks like on Database and Files levels.  Quick way to get information on your table is to run `DESCRIBE EXTENDED` command on SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED apj_locations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC One of the rows in output above provides us with `Location` information. This is where the actual delta files are being stored.
# MAGIC 
# MAGIC Now that we know the location of this table, let's look at the files! We can use another `dbutils` command for that

# COMMAND ----------

table_location = f"dbfs:/user/hive/warehouse/{database_name}.db/apj_locations"

print(f"Make sure this match your table location as per DESCRIBE EXTENDED output above: {table_location}") 

dbutils.fs.ls(table_location)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC It is a bit hard to read - what if we use Spark and push data to the DataFrame? We can create a python function to do this so we can use it again in the future!

# COMMAND ----------

def show_files_as_dataframe(files_location):

  from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, LongType

  # Assign dbutils output to a variable
  dbutils_output = dbutils.fs.ls(files_location)

  # Convert output to RDD
  rdd = spark.sparkContext.parallelize(dbutils_output)

  # Create a schema for this dataframe
  schema = StructType([
      StructField('path', StringType(), True),
      StructField('name', StringType(), True),
      StructField('size', IntegerType(), True),
      StructField('modificationTime', LongType(), True)
  ])

  # Create data frame
  files_df = spark.createDataFrame(rdd,schema)
  return files_df

# Call our new function to see current files
show_files_as_dataframe(table_location).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explore Delta Log
# MAGIC 
# MAGIC We can see that next to data stored in parquet file we have a *_delta_log/* folder - this is where the Log files can be found

# COMMAND ----------

log_files_location = f"{table_location}/_delta_log/"

show_files_as_dataframe(log_files_location).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC `00000000000000000000.json` has a very first commit logged for our table. Each change to the table will be creating a new _json_ file

# COMMAND ----------

first_log_file_location = f"{log_files_location}00000000000000000000.json"
dbutils.fs.head(first_log_file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC It has a lot of useful information, but yet again it is not easy to read. We can try to query this _json_ file directly using Spark SQL to see if we can push this data to tabular view

# COMMAND ----------


log_df = spark.read.json(first_log_file_location)

display(log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MANAGED Tables
# MAGIC 
# MAGIC This is a MANAGED table - we can see it in DESCRIBE EXTENDED results as well as in the transaction log above. A _managed table_ is a Spark SQL table for which Spark manages both the data and the metadata. In the case of managed table, Databricks stores the metadata and data in DBFS in your account. Since Spark SQL manages the tables, running a DROP TABLE command deletes **both** the metadata and data.
# MAGIC 
# MAGIC Let's try it using a different way to run SQL statements - by using spark.sql() command in a python cell

# COMMAND ----------

spark.sql("DROP TABLE apj_locations");

try:
  dbutils.fs.ls(table_location)
except Exception as e:
  print(e)
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Another option is to let Spark SQL manage the metadata, while you control the data location. We refer to this as an unmanaged or **external table**. Spark SQL manages the relevant metadata, so when you perform DROP TABLE, Spark removes only the metadata and not the data itself. The data is still present in the path you provided.
# MAGIC 
# MAGIC Let's re-create our `bronze_apj_locations`, this time as an external table. `df` still has our initial DataFrame so let's use it and save it as a table

# COMMAND ----------

df.write \
  .mode("overwrite") \
  .option("path", apj_locations_data_path) \
  .saveAsTable("apj_locations")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE EXTENDED apj_locations;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we drop table now - the data remains on our storage location.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS apj_locations

# COMMAND ----------

show_files_as_dataframe(apj_locations_data_path).display();

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We will need this table however so let's create a Delta Table pointing to existing location

# COMMAND ----------

spark.sql(f"CREATE TABLE apj_locations LOCATION '{apj_locations_data_path}'")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Update Delta Table
# MAGIC 
# MAGIC Provided dataset has address information, but no country name - let's add one!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC alter table apj_locations
# MAGIC add column store_country string;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update apj_locations
# MAGIC set store_country = case when id in ('SYD01', 'MEL01', 'BNE02') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end

# COMMAND ----------

# MAGIC %sql 
# MAGIC select store_country, id, store_name from apj_locations

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update apj_locations
# MAGIC set store_country = 'AUS'
# MAGIC where id = 'MEL02'

# COMMAND ----------

# MAGIC %sql
# MAGIC select store_country, count(id) as number_of_stores from apj_locations group by store_country

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Track Data History
# MAGIC 
# MAGIC 
# MAGIC Delta Tables keep all changes made in the delta log we've seen before. There are multiple ways to see that - e.g. by running `DESCRIBE HISTORY` for a table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY apj_locations

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC We can also check what files storage location has now

# COMMAND ----------

show_files_as_dataframe(apj_locations_data_path).display();

# COMMAND ----------

show_files_as_dataframe(apj_locations_data_path + "/_delta_log").display();

# COMMAND ----------

# display log for version 3

display(spark.read.json(apj_locations_data_path + "/_delta_log/00000000000000000003.json"))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Having all this information and old data files mean that we can **Time Travel**!  You can query your table at any given `VERSION AS OF` or  `TIMESTAMP AS OF`.
# MAGIC 
# MAGIC Let's check again what table looked like before we ran last update

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from apj_locations VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can remove files no longer referenced by a Delta table and are older than the retention threshold by running the `VACCUM` command on the table. vacuum is not triggered automatically. The default retention threshold for the files is 7 days.
# MAGIC 
# MAGIC `vacuum` deletes only data files, not log files. Log files are deleted automatically and asynchronously after checkpoint operations. The default retention period of log files is 30 days, configurable through the `delta.logRetentionDuration` property which you set with the `ALTER TABLE SET TBLPROPERTIES` SQL method.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC ALTER TABLE apj_locations
# MAGIC SET TBLPROPERTIES ('delta.logRetentionDuration' = '180 Days')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Change Data Feed
# MAGIC 
# MAGIC 
# MAGIC MORE INFO ON HOW AND WHY.
# MAGIC 
# MAGIC It is not turned ON by default, but we can enabled it using `TBLPROPERTIES`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE apj_locations SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe history apj_locations

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM table_changes('apj_locations', 9)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update apj_locations
# MAGIC set store_address = 'Domestic Terminal'
# MAGIC where id = 'AKL01';
# MAGIC 
# MAGIC SELECT * FROM table_changes('apj_locations', 10) -- Note that we increment version by 1 due to UPDATE statement above

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ELT Workload
# MAGIC 
# MAGIC TODO: introduce incremental updates as more real-life scenario

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![ ](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Autoloader
# MAGIC 
# MAGIC In hour example most of the locations upload their sales data to our storage location on nightly batches. Some of them however have upgraded to hourly or even more frequent data feeds.
# MAGIC 
# MAGIC Easy way to bring incremental data to our Delta Lake is by using **autoloader**.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # create a simple autolaoder to read data files 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As you can see in the cell above, autoloader is now running in Streaming mode and is listening to new files being uploaded.
# MAGIC 
# MAGIC Let simulate SYD01 location uploading a new data file

# COMMAND ----------

# TODO: write a function to upload file to autoloader location

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: check that new records are uploaded

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Our APJ locations are only sending files on daily or hourly frequency so in this case running autolaoder as a stream migth not be cost effective.
# MAGIC 
# MAGIC Lucky for us, it is very easy to switch autoloader to batch mode, all it takes is to change Trigger mode 
# MAGIC 
# MAGIC 
# MAGIC ** TODO ** Trigger Once or available now

# COMMAND ----------

#TODO: stop all streams to automate this part

# COMMAND ----------

# TODO: add autoloader in batch mode, create more data files to ingest and show how new ones are being picked up

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Silver Tables

# COMMAND ----------

# create a silver table as select * with some data cleansing + another one for item level info

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This is great for one-off run, but what if we want to keep updating table every day with new data only?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### MERGE

# COMMAND ----------

# show how merge into works for both silver tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Gold Tables

# COMMAND ----------

# Gold table for sales by country
# Check if we can create something to be used for ML / BI demos

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Scheduled Updates

# COMMAND ----------

# more info on Jobs and how to create one
