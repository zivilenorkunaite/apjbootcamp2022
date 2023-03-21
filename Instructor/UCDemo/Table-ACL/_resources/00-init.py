# Databricks notebook source
# MAGIC %md 
# MAGIC #Permission-setup Data generation for UC demo notebook

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

folder = "/demo/uc/"

print("generating the data...")
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
import random
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_id = F.udf(lambda: str(uuid.uuid4()))
countries = ['FR', 'USA', 'SPAIN']
fake_country = F.udf(lambda: countries[random.randint(0,2)])

df = spark.range(0, 10000)
#TODO: need to increment ID for each write batch to avoid duplicate. Could get the max reading existing data, zero if none, and add it ti the ID to garantee almost unique ID (doesn't have to be perfect)  
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())
df = df.withColumn("customer_firstname", fake_firstname())
df = df.withColumn("customer_lastname", fake_lastname())
df = df.withColumn("country", fake_country())
df = df.withColumn("customer_email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2))
df = df.withColumn("age_group", F.round(F.rand()*10))
df.repartition(3).write.mode('overwrite').format("json").save(folder+"users")

# COMMAND ----------

workspace_users = [u.name[:-1].lower() for u in dbutils.fs.ls("/Users") if '@databricks' in u.name]
user_data = [(u, countries[random.randint(0,2)],random.randint(0,1)) for u in workspace_users]
spark.createDataFrame(user_data, ['analyst_email', 'country_filter',"gdpr_filter"]).repartition(3).write.mode('overwrite').format("json").save(folder+"analyst_permissions")

# COMMAND ----------

import pyspark.sql.functions as F
import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

catalog = "uc_demos_"+current_user_no_at

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

database = 'uc_acl'
db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == database]) == 0
if db_not_exist:
  print(f"creating {database} database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
  spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uc_acl.analyst_permissions (
# MAGIC   analyst_email STRING,
# MAGIC   country_filter STRING,
# MAGIC   gdpr_filter LONG); 
# MAGIC 
# MAGIC MERGE INTO uc_acl.analyst_permissions a
# MAGIC USING (select * FROM json.`/demo/uc/analyst_permissions`) as u
# MAGIC ON a.analyst_email = u.analyst_email
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 
# MAGIC MERGE INTO uc_acl.analyst_permissions a
# MAGIC USING (select current_user() as analyst_email, 'FR' as country_filter, false gdpr_filter) as u
# MAGIC ON a.analyst_email = u.analyst_email
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 
# MAGIC -- ALTER TABLE uc_acl.users OWNER TO `account users`;
# MAGIC ALTER TABLE uc_acl.analyst_permissions OWNER TO `account users`;