-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables with SQL
-- MAGIC 
-- MAGIC This notebook uses SQL to declare Delta Live Tables. 
-- MAGIC 
-- MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic DLT SQL Syntax
-- MAGIC 
-- MAGIC At its simplest, you can think of DLT SQL as a slight modification to tradtional CTAS statements.
-- MAGIC 
-- MAGIC DLT tables and views will always be preceded by the `LIVE` keyword.
-- MAGIC 
-- MAGIC If you wish to process data incrementally (using the same processing model as Structured Streaming), also use the `STREAMING` keyword.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Step 1: Create Bronze table for Sales

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_sales
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze sales table with all transactions"
AS 
SELECT * 
FROM
cloud_files( '/tmp/databricksbootcamp/datasets/sales/' , "json") 

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_stores
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Store locations dimension"
AS 
SELECT *, case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code 
FROM  
cloud_files('/tmp/databricksbootcamp/datasets/stores/' , 'json');

-- COMMAND ----------

-- This table is different - it gets data as part of CDC feed from our source system
CREATE STREAMING LIVE TABLE bronze_products
TBLPROPERTIES ("quality" = "cdc")
COMMENT "CDC records for our products dataset"
AS 
SELECT * FROM 
cloud_files( '/tmp/databricksbootcamp/datasets/products_cdc/' , "json") ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Weather data
-- MAGIC 
-- MAGIC We also have some data from weather API - it can be transformed using a different notebook, but we can also add it here.
-- MAGIC 
-- MAGIC **Important**
-- MAGIC 
-- MAGIC Change data path to match one you had in `01 - Ingest notebook` to pick up your dataset. You can get it by running cell below

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC weather_files_location = f"/tmp/{current_user_id}/datasets/weather/"
-- MAGIC print(weather_files_location)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_weather
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Records from weather api"
AS 
SELECT * FROM 
cloud_files( '/tmp/databricksbootcamp/datasets/weather/' , "json") ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Step 2: Create a Silver table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Referencing Streaming Tables
-- MAGIC 
-- MAGIC Queries against other DLT tables and views will always use the syntax `live.table_name`. At execution, the target database name will be substituted, allowing for easily migration of pipelines between DEV/QA/PROD environments.
-- MAGIC 
-- MAGIC When referring to another streaming DLT table within a pipeline, use the `STREAM(live.table_name)` syntax to ensure incremental processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quality Control with Constraint Clauses
-- MAGIC 
-- MAGIC Data expectations are expressed as simple constraint clauses, which are essential where statements against a field in a table.
-- MAGIC 
-- MAGIC Adding a constraint clause will always collect metrics on violations. If no `ON VIOLATION` clause is included, records violating the expectation will still be included.
-- MAGIC 
-- MAGIC DLT currently supports two options for the `ON VIOLATION` clause.
-- MAGIC 
-- MAGIC | mode | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `FAIL UPDATE` | Fail when expectation is not met |
-- MAGIC | `DROP ROW` | Only process records that fulfill expectations |
-- MAGIC | ` ` | Alert, but still process |
-- MAGIC 
-- MAGIC 
-- MAGIC Roadmap: `QUARANTINE`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver fact tables

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_sales_clean (
  CONSTRAINT `Location has to be 5 characters long` EXPECT (length(store_id) = 5),
  CONSTRAINT `Only CANCELED and COMPLETED transactions are allowed` EXPECT (order_state IN ('CANCELED', 'COMPLETED'))
) 
TBLPROPERTIES ("quality" = "silver")
COMMENT "Silver table with clean transaction records" AS
  SELECT
    id as id,
    ts as ts,
    store_id as store_id,
    customer_id as customer_id,
    store_id || "-" || cast(customer_id as string) as unique_customer_id,
    order_source as order_source,
    STATE as order_state,
    sale_items as sale_items
  from STREAM(live.bronze_sales)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales;

-- Use APPLY CHANGES INTO to keep only the most rec
APPLY CHANGES INTO LIVE.silver_sales
  FROM 
  stream(live.silver_sales_clean)
  KEYS (id)
  SEQUENCE BY ts


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Dimension tables in Silver layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Silver layer is a good place to add some data quality expectations

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_stores  (
  CONSTRAINT `Location has to be 5 characters long` EXPECT (length(id) = 5)
  )
  TBLPROPERTIES ("quality" = "silver")
AS
SELECT * from STREAM(live.bronze_stores)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Our silver_products table will be tracking changes history by using SCD TYPE 2 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_products;

-- Use APPLY CHANGES INTO to keep only the history as well
APPLY CHANGES INTO LIVE.silver_products
  FROM 
  stream(live.bronze_products)
  KEYS (id)
  IGNORE NULL UPDATES
  APPLY AS DELETE WHEN _change_type = 'delete'
  SEQUENCE BY _change_timestamp
  COLUMNS  * EXCEPT (_change_type, _change_timestamp, _rescued_data)
  STORED AS SCD TYPE 2


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Weather data needs some cleanup to convert it to multiple rows

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_weather_clean AS
select
  concat_ws(latitude, longitude, t.time) as pk,
  latitude,
  longitude,
  timezone,
  generationtime_ms,
  t.time,
  t.temperature_2m,
  t.rain
from
  (
    select
      latitude,
      longitude,
      timezone,
      generationtime_ms,
      explode(
        arrays_zip(hourly.time, hourly.temperature_2m, hourly.rain)
      ) as t
    from
      (
        select
          latitude,
          longitude,
          timezone,
          generationtime_ms,
          from_json(
            hourly,
            'struct<time:array<timestamp>,temperature_2m:array<decimal>,rain:array<decimal>>'
          ) as hourly
        from
          STREAM(live.bronze_weather)
      )
  )

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Hands On Task!
-- MAGIC 
-- MAGIC Finish the cell below to have a silver_weather table with unique and up to date records by using APPLY CHANGES INTO and using table `live.silver_weather_clean`
-- MAGIC 
-- MAGIC For this table we do not need to track history - keep it as SCD Type 1

-- COMMAND ----------

-- REMOVE COMMENT AND FINISH WRITING THIS SQL

-- CREATE OR REFRESH STREAMING LIVE TABLE silver_weather;

-- APPLY CHANGES INTO LIVE.silver_weather FROM 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Step 3: Create Gold tables
-- MAGIC 
-- MAGIC These tables will be used by your business users and will usually contain aggregated datasets

-- COMMAND ----------

CREATE LIVE TABLE country_sales
select l.country_code, count(distinct s.id) as number_of_sales
from live.silver_sales s 
  join live.silver_stores l on s.store_id = l.id
group by l.country_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Hands On Task!
-- MAGIC 
-- MAGIC Create 2 more gold tables that would be using any of the existing silver ones and check how they appear on your DLT pipeline
-- MAGIC 
-- MAGIC 
-- MAGIC ### Advanced option
-- MAGIC 
-- MAGIC Create another gold table, but this time using python. Note - you will need to use a new notebook for it and later add it to your existing DLT pipeline.
-- MAGIC 
-- MAGIC You can also create a silver_sales_item table with each row containing information about specific juice sold and get more insights about most popular combinations!
