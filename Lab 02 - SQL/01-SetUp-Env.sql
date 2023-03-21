-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create a database to store your tables.
-- MAGIC </br>
-- MAGIC In the following cell replace "your_unique_database_name" with a unique database name :) (e.g., first_name_last_name_date). 
-- MAGIC </br>
-- MAGIC Make sure you don't remove the ;

-- COMMAND ----------

CREATE DATABASE your_unique_database_name;
USE your_unique_database_name;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC datasets_location = f'/FileStore/tmp/{current_user_id}/datasets/'
-- MAGIC 
-- MAGIC dbutils.fs.rm(datasets_location, True)
-- MAGIC print(current_user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
-- MAGIC spark.sql(f'create database if not exists {database_name};')
-- MAGIC spark.sql(f'use {database_name}')
-- MAGIC print(database_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # copy sample data from git
-- MAGIC 
-- MAGIC import os
-- MAGIC 
-- MAGIC working_dir = '/'.join(os.getcwd().split('/')[0:5])
-- MAGIC git_datasets_location = f'{working_dir}/Datasets/SQL Lab'
-- MAGIC 
-- MAGIC sample_datasets  =['dim_products','fact_apj_sale_items','dim_store_locations', 'fact_apj_sales_fact']
-- MAGIC for sample_data in sample_datasets:
-- MAGIC   dbutils.fs.cp(f'file:{git_datasets_location}/{sample_data}.csv', f'{datasets_location}/SQL_Lab/{sample_data}.csv')
-- MAGIC 
-- MAGIC spark.conf.set('sampledata.path',f'{datasets_location}SQL_Lab/')
-- MAGIC spark.conf.set('sampledata.path','dbfs:/FileStore/anzbootcamp/dim_products.csv')

-- COMMAND ----------

select concat('${sampledata.path}', 'dim_products.csv')

-- COMMAND ----------

DROP TABLE IF EXISTS dim_products;

CREATE  TABLE IF NOT EXISTS dim_products;

COPY INTO dim_products FROM 
(SELECT *
FROM '${sampledata.path}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

/*dim_products*/
DROP TABLE IF EXISTS dim_products;

CREATE  TABLE IF NOT EXISTS dim_products;

COPY INTO dim_products FROM 
(SELECT *
FROM 'dbfs:/FileStore/anzbootcamp/dim_products.csv')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


/*dim_store_locations*/
DROP TABLE IF EXISTS dim_store_locations;

CREATE  TABLE IF NOT EXISTS dim_store_locations;

COPY INTO dim_store_locations FROM 
(SELECT *
FROM 'dbfs:/FileStore/anzbootcamp/dim_store_locations_sample.csv')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


/*apj_sales_fact*/
DROP TABLE IF EXISTS apj_sales_fact;

CREATE  TABLE IF NOT EXISTS apj_sales_fact;

COPY INTO apj_sales_fact FROM 
(SELECT *
FROM 'dbfs:/FileStore/anzbootcamp/apj_sales_fact_sample.csv')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


/*apj_sale_items*/
DROP TABLE IF EXISTS apj_sale_items_fact;

CREATE  TABLE IF NOT EXISTS apj_sale_items_fact;

COPY INTO apj_sale_items_fact FROM 
(SELECT *
FROM 'dbfs:/FileStore/anzbootcamp/apj_sale_items_fact.csv')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


/*store_data, json*/
DROP TABLE IF EXISTS store_data;
CREATE TABLE IF NOT EXISTS store_data AS SELECT

1 AS id, '{
   "store":{
      "fruit": [
        {"weight":8,"type":"apple"},
        {"weight":9,"type":"pear"}
      ],
      "basket":[
        [1,2,{"b":"y","a":"x"}],
        [3,4],
        [5,6]
      ],
      "book":[
        {
          "author":"Nigel Rees",
          "title":"Sayings of the Century",
          "category":"reference",
          "price":8.95
        },
        {
          "author":"Herman Melville",
          "title":"Moby Dick",
          "category":"fiction",
          "price":8.99,
          "isbn":"0-553-21311-3"
        },
        {
          "author":"J. R. R. Tolkien",
          "title":"The Lord of the Rings",
          "category":"fiction",
          "reader":[
            {"age":25,"name":"bob"},
            {"age":26,"name":"jack"}
          ],
          "price":22.99,
          "isbn":"0-395-19395-8"
        }
      ],
      "bicycle":{
        "price":19.95,
        "color":"red"
      }
    },
    "owner":"amy",
    "zip code":"94025",
    "fb:testid":"1234"
 }' as raw



