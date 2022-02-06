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
-- MAGIC If you wish to process data incrementally (using the same processing model as Structured Streaming), also use the `INCREMENTAL` keyword.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Step 1: Create Bronze table for Sales

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE bronze_sales_dlt
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze sales table with all transactions"
AS 
SELECT * 
FROM
cloud_files( '/FileStore/${mypipeline.data_path}/deltademoasset/dlt_ingest/' , "json") 

-- COMMAND ----------

-- TEMPORARY TABLES will only be available for the pipeline and won't show up in target database
CREATE TEMPORARY LIVE TABLE dim_users_dlt
TBLPROPERTIES ("quality" = "lookup")
COMMENT "Users dimension - not included in database"
AS 
SELECT store_id || "-" || cast(id as string) as unique_id, id, store_id, name, email 
FROM 
json.`/FileStore/${mypipeline.data_path}/deltademoasset/users.json`;


-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE dim_locations_dlt
TBLPROPERTIES ("quality" = "lookup")
COMMENT "Store locations dimension - not included in database"
AS 
SELECT *, case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code 
FROM  
json.`/FileStore/${mypipeline.data_path}/deltademoasset/stores.json`;

-- COMMAND ----------

CREATE LIVE TABLE dim_products_dlt
TBLPROPERTIES ("quality" = "lookup")
COMMENT "Products dimension "
AS 
SELECT * FROM  json.`/FileStore/${mypipeline.data_path}/deltademoasset/products.json`;

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
-- MAGIC 
-- MAGIC 
-- MAGIC Roadmap: `QUARANTINE`

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE silver_sales_dlt (
  CONSTRAINT `Location has to be 5 characters long` EXPECT (length(store_id) = 5),
  CONSTRAINT `Only CANCELED and COMPLETED transactions are allowed` EXPECT (order_state IN ('CANCELED', 'COMPLETED'))
) 
TBLPROPERTIES ("quality" = "silver")
COMMENT "Silver table with clean transaction records" AS
  SELECT
    saleID as id,
    from_unixtime(ts) as ts,
    Location as store_id,
    CustomerID as customer_id,
    location || "-" || cast(CustomerID as string) as unique_customer_id,
    OrderSource as order_source,
    STATE as order_state,
    SaleItems as sale_items
  from STREAM(live.bronze_sales_dlt)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE silver_sale_items_dlt (
  CONSTRAINT `All custom juice must have ingredients` EXPECT (NOT(product_id = 'Custom' and size(product_ingredients) = 0))
) 
TBLPROPERTIES ("quality" = "silver")
COMMENT "Silver table with clean transaction records" AS

SELECT
    id || "-" || cast(pos as string) as id,
    id as sale_id,
    store_id,
    pos as item_number,
    col.id as product_id,
    col.size as product_size,
    col.notes as product_notes,
    col.cost as product_cost,
    col.ingredients as product_ingredients
  from
    (
  select
    *,
    posexplode(
      from_json(
        sale_items,
        'array<struct<id:string,size:string,notes:string,cost:double,ingredients:array<string>>>'
      )
    ) 
  from
    (
      SELECT
    saleID as id,
    from_unixtime(ts) as ts,
    Location as store_id,
    CustomerID as customer_id,
    location || "-" || cast(CustomerID as string) as unique_customer_id,
    OrderSource as order_source,
    STATE as order_state,
    SaleItems as sale_items
  from STREAM(live.bronze_sales_dlt)
    )
)


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Step 3: Create Gold table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Enrich dataset with lookup table

-- COMMAND ----------

CREATE LIVE TABLE country_sales_dlt
select l.country_code, sum(product_cost) as total_sales, count(distinct sale_id) as number_of_sales
from live.silver_sale_items_dlt s 
  join live.dim_locations_dlt l on s.store_id = l.id
group by l.country_code;

-- COMMAND ----------

CREATE LIVE TABLE country_monthly_sales_dlt
select l.country_code, date_format(sales.ts, 'yyyy-MM') as sales_month, sum(product_cost) as total_sales, count(distinct sale_id) as number_of_sales
from live.silver_sale_items_dlt s 
  join live.dim_locations_dlt l on s.store_id = l.id
  join live.silver_sales_dlt sales on s.sale_id = sales.id
group by l.country_code, date_format(sales.ts, 'yyyy-MM');

-- COMMAND ----------

CREATE LIVE TABLE user_profile_dlt
COMMENT "All current assest belonging to user"
select s.store_id, ss.unique_customer_id, c.name, sum(product_cost) total_spend 
from live.silver_sale_items_dlt s 
  join live.silver_sales_dlt ss on s.sale_id = ss.id
  join live.dim_users_dlt c on ss.unique_customer_id = c.unique_id
where ss.unique_customer_id is not null 
group by s.store_id, ss.unique_customer_id, c.name
