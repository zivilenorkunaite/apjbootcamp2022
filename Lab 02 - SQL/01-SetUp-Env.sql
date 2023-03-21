-- Databricks notebook source
-- MAGIC %python
-- MAGIC import os
-- MAGIC 
-- MAGIC reset = True
-- MAGIC current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC datasets_location = f'/FileStore/tmp/{current_user_id}/datasets/'
-- MAGIC database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
-- MAGIC 
-- MAGIC if reset:
-- MAGIC   dbutils.fs.rm(datasets_location, True)
-- MAGIC   spark.sql(f'DROP DATABASE IF EXISTS {database_name} CASCADE')
-- MAGIC 
-- MAGIC 
-- MAGIC # create database
-- MAGIC spark.sql(f'CREATE DATABASE IF NOT EXISTS {database_name};')
-- MAGIC spark.sql(f'USE {database_name}')
-- MAGIC 
-- MAGIC # copy sample data from git
-- MAGIC 
-- MAGIC working_dir = '/'.join(os.getcwd().split('/')[0:5])
-- MAGIC git_datasets_location = f'{working_dir}/Datasets/SQL Lab'
-- MAGIC 
-- MAGIC sample_datasets  =['dim_products','fact_apj_sale_items','dim_store_locations', 'fact_apj_sales']
-- MAGIC for sample_data in sample_datasets:
-- MAGIC   dbutils.fs.cp(f'file:{git_datasets_location}/{sample_data}.csv', f'{datasets_location}/SQL_Lab/{sample_data}.csv')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###GET the DATABASE NAME below
-- MAGIC You should use this throughout the lab

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f'Use this database name through out the lab: {database_name}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_name = "dim_products"
-- MAGIC sample_file = f"{table_name}.csv"
-- MAGIC spark.conf.set('sampledata.path',f'dbfs:{datasets_location}SQL_Lab/{sample_file}')
-- MAGIC spark.conf.set('table.name', table_name)

-- COMMAND ----------

DROP TABLE IF EXISTS `${table.name}`;

CREATE TABLE IF NOT EXISTS `${table.name}`;

COPY INTO `${table.name}` FROM 
(SELECT *
FROM  '${sampledata.path}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true',
                'quote'="'")
COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM `${table.name}`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_name = "dim_store_locations"
-- MAGIC sample_file = f"{table_name}.csv"
-- MAGIC spark.conf.set('sampledata.path',f'dbfs:{datasets_location}SQL_Lab/{sample_file}')
-- MAGIC spark.conf.set('table.name', table_name)

-- COMMAND ----------

DROP TABLE IF EXISTS `${table.name}`;

CREATE TABLE IF NOT EXISTS `${table.name}`;

COPY INTO `${table.name}` FROM 
(SELECT *
FROM  '${sampledata.path}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true',
                'quote'="'")
COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM `${table.name}`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_name = "fact_apj_sales"
-- MAGIC sample_file = f"{table_name}.csv"
-- MAGIC spark.conf.set('sampledata.path',f'dbfs:{datasets_location}SQL_Lab/{sample_file}')
-- MAGIC spark.conf.set('table.name', table_name)

-- COMMAND ----------

DROP TABLE IF EXISTS `${table.name}`;

CREATE TABLE IF NOT EXISTS `${table.name}`;

COPY INTO `${table.name}` FROM 
(SELECT *
FROM  '${sampledata.path}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true',
                'quote'="'")
COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM `${table.name}`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_name = "fact_apj_sale_items"
-- MAGIC sample_file = f"{table_name}.csv"
-- MAGIC spark.conf.set('sampledata.path',f'dbfs:{datasets_location}SQL_Lab/{sample_file}')
-- MAGIC spark.conf.set('table.name', table_name)

-- COMMAND ----------

DROP TABLE IF EXISTS `${table.name}`;

CREATE TABLE IF NOT EXISTS `${table.name}`;

COPY INTO `${table.name}` FROM 
(SELECT *
FROM  '${sampledata.path}')
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true',
                'delimiter' = ',',
                'header' = 'true',
                'quote'="'")
COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM `${table.name}`;

-- COMMAND ----------

/*store_data, json*/
CREATE OR REPLACE TABLE store_data_json
AS SELECT
1 AS id,
'{
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
 }' as raw;

SELECT * FROM store_data_json;

