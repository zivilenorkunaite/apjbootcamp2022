# Databricks notebook source
# MAGIC %md
# MAGIC ### AP Juice Lakehouse Platform
# MAGIC 
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />
# MAGIC 
# MAGIC 
# MAGIC For this exercise we will be creating a data model in the gold layer in Lakehouse platform for our company, AP Juice.
# MAGIC 
# MAGIC AP Juice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
# MAGIC 
# MAGIC For this part of the exercise we will be processing 3 existing dimensions and sales transactions datasets. Files will be a mix of `csv` and `json` files and our goal is to have modelled gold layer

# COMMAND ----------

# MAGIC %run ./Utils/Fetch-User-Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/ANZBootcampBatch.jpg?raw=true" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explore what we have in the Silver Layer and the dim tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE <>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Show tables in <>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from silver_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from silver_sale_items

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from silver_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from silver_products

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from silver_store_locations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://i.ibb.co/zQHhFcg/modelling.png'>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Customer Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customers
# MAGIC as select row_number() over (ORDER BY unique_id) as customer_skey , unique_id, store_id, name, email,'Y' as current_record,cast('1900-01-01 00:00:00'as timestamp) as start_date, cast(null as timestamp) as end_date
# MAGIC from silver_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Product Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_products
# MAGIC as select row_number() over (ORDER BY id) as product_skey , id, ingredients, name,'Y' as current_record,cast('1900-01-01 00:00:00'as timestamp) as start_date, cast(null as timestamp) as end_date
# MAGIC from silver_products

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dim_products

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Store Location Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_store_locations
# MAGIC as select row_number() over (ORDER BY id) as slocation_skey, id, name, city, hq_address, country_code, phone_number, 'Y' as current_record,cast('1900-01-01 00:00:00'as timestamp) as start_date, cast(null as timestamp) as end_date
# MAGIC from silver_store_locations

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dim_store_locations

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create APJ Sales Fact

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table apj_sales_fact
# MAGIC With apj_sales_fact_tmp as (Select f.id as sale_id, f.ts, f.order_source, f.order_state, f.unique_customer_id, f.store_id
# MAGIC from  silver_sales f
# MAGIC )
# MAGIC 
# MAGIC Select dc.customer_skey as customer_skey, dsl.slocation_skey as slocation_skey, f.* from apj_sales_fact_tmp f
# MAGIC 
# MAGIC /* Get the Customer SKEY record */
# MAGIC 
# MAGIC join dim_customers dc
# MAGIC on f.unique_customer_id = dc.unique_id
# MAGIC 
# MAGIC /* Get the Location SKEY record */
# MAGIC join dim_store_locations dsl 
# MAGIC on f.store_id = dsl.id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create APJ Sale Items Fact

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table apj_sale_items_fact
# MAGIC With apj_sale_items_fact_tmp as (Select f.sale_id, f.product_id, f.store_id, f.product_size, f.product_cost, f.product_ingredients
# MAGIC from  silver_sale_items f
# MAGIC )
# MAGIC 
# MAGIC Select dp.product_skey as product_skey, dsl.slocation_skey as slocation_skey, ss.unique_customer_id,  f.* from apj_sale_items_fact_tmp f
# MAGIC 
# MAGIC /* Get the Product SKEY record */
# MAGIC 
# MAGIC join dim_products dp
# MAGIC on f.product_id = dp.id
# MAGIC 
# MAGIC /* Get the Location SKEY record */
# MAGIC join dim_store_locations dsl 
# MAGIC on f.store_id = dsl.id
# MAGIC 
# MAGIC join silver_sales ss
# MAGIC on f.sale_id = ss.id

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from apj_sales_fact

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from apj_sale_items_fact
