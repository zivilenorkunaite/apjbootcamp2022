-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Databricks Unity Catalog - Table ACL
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/us-base-0.png" style="float: right" width="500px"/> 
-- MAGIC 
-- MAGIC The main feature of Unity Catalog is to provide you an easy way to setup Table ACL (Access Control Level), but also build Dynamic Views based on each individual permission.
-- MAGIC 
-- MAGIC Typically, Analysts will only have access to customers from their country and won't be able to read GDPR/Sensitive informations (like email, firstname etc.)
-- MAGIC 
-- MAGIC A typical workflow in the Lakehouse architecture is the following:
-- MAGIC 
-- MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
-- MAGIC * Data Scientists can read the final tables and update their features tables
-- MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
-- MAGIC * Data is masked/anonymized dynamically based on each user access level
-- MAGIC 
-- MAGIC With Unity Catalog, your tables, users and groups are defined at the account level, cross workspaces. Ideal to deploy and operate a Lakehouse Platform across all your teams.
-- MAGIC 
-- MAGIC Let's see how this can be done with the Unity Catalog
-- MAGIC 
-- MAGIC <!-- tracking, please do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Ftable_acl%2Facl&dt=FEATURE_UC_TABLE_ACL">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC 
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC 
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- MAGIC %run ./_resources/00-init

-- COMMAND ----------

-- MAGIC %fs ls /

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Creating the CATALOG
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC The first step is to create a new catalog.
-- MAGIC 
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC 
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC 
-- MAGIC To access one table, you can specify the full path: ```SELECT * FROM <CATALOG>.<SCHEMA>.<TABLE>```
-- MAGIC 
-- MAGIC Note that the tables created before Unity Catalog are saved under the catalog named `hive_metastore`. Unity Catalog features are not available for this catalog.
-- MAGIC 
-- MAGIC Note that Unity Catalog comes in addition to your existing data, not hard change required!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"The demo will create and use the catalog {catalog}:")
-- MAGIC spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
-- MAGIC spark.sql(f"USE CATALOG {catalog}")

-- COMMAND ----------

-- the catalog has been created for your user and is defined as default. All shares will be created inside.
-- make sure you run the 00-setup cell above to init the catalog to your user. 
SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating the SCHEMA
-- MAGIC Next, we need to create the SCHEMA (or DATABASE).
-- MAGIC 
-- MAGIC Unity catalog provide the standard GRANT SQL syntax. We'll use it to GRANT CREATE and USAGE on our SCHEMA to all the users for this demo.
-- MAGIC 
-- MAGIC They'll be able to create extra table into this schema.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS uc_acl;
USE uc_acl;

-- COMMAND ----------

-- DBTITLE 1,Let's make sure that all users can use the uc_acl schema for our demo:
GRANT CREATE, USAGE ON SCHEMA uc_acl TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating our table
-- MAGIC 
-- MAGIC We're all set! We can use standard SQL to create our tables.
-- MAGIC 
-- MAGIC We'll use a customers dataset, loading data about users (id, email etc...)
-- MAGIC 
-- MAGIC Because we want our demo to be available for all, we'll grant full privilege to the table to all USERS.
-- MAGIC 
-- MAGIC Note that the table owner is the current user. Owners have full permissions.<br/>
-- MAGIC If you want to change the owner you can set it as following: ```ALTER TABLE <catalog>.uc_acl.customers OWNER TO `account users`;```

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS uc_acl.customers (
  id BIGINT,
  creation_date STRING,
  customer_firstname STRING,
  customer_lastname STRING,
  country STRING,
  customer_email STRING,
  address STRING,
  gender DOUBLE,
  age_group DOUBLE); 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Loading some data
-- MAGIC 
-- MAGIC We can use plain SQL to load more data into our new customer table. `COPY INTO` can do that easily. 
-- MAGIC 
-- MAGIC Note that this is idempotent: if you run it twice it will only copy new data from your folder into the table.

-- COMMAND ----------

COPY INTO uc_acl.customers  FROM '/demo/uc/users' FILEFORMAT=JSON 

-- COMMAND ----------

SELECT * FROM  uc_acl.customers

-- COMMAND ----------

-- MAGIC %md ## Granting user access
-- MAGIC 
-- MAGIC Let's now use Unity Catalog to GRANT permission on the table.
-- MAGIC 
-- MAGIC Unity catalog let you GRANT standard SQL permission to your objects, using the Unity Catalog users or group:

-- COMMAND ----------

-- Let's grant all users a SELECT
GRANT SELECT ON TABLE uc_acl.customers TO `account users`;

-- We'll grant an extra MODIFY to our Data Engineer
GRANT SELECT, MODIFY ON TABLE uc_acl.customers TO `dataengineers`;

-- COMMAND ----------

SHOW GRANTS ON TABLE uc_acl.customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC Unity Catalog gives you Table ACL permissions, leveraging users, group and table across multiple workspaces.
-- MAGIC 
-- MAGIC But UC not only gives you control over Tables. You can do more advanced permission and data access pattern such as dynamic masking at the row level.
-- MAGIC 
-- MAGIC Let's see how this can be done using dynamic view in the [next notebook]($./01-UC-Dynamic-view)