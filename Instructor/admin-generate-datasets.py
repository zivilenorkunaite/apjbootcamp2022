# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Generate dataset for individual location, day by day. Add working hours as input, maybe also hours with more sales.
# MAGIC For products - create object for existing ones and auto-generate "Custom" ones. This can be added later on.
# MAGIC 
# MAGIC Price - have standard price by size, no matter what ingredients are being used. Add extra $1 for Custom option
# MAGIC 
# MAGIC 
# MAGIC * Record ID - unique id
# MAGIC * Timestamp  - epoch
# MAGIC * Location id
# MAGIC * An array for sale:
# MAGIC     * product ID [juice name or Custom]
# MAGIC     * size
# MAGIC     * Ingredients
# MAGIC     * Notes [e.g. no ice etc]
# MAGIC     * Item Cost [discounted sales?]
# MAGIC * Payment method [cash, card, online]
# MAGIC * State [completed, canceled]
# MAGIC * Order source [online / in-store]
# MAGIC * Customer id [optional]

# COMMAND ----------

!pip install git+https://github.com/databrickslabs/dbldatagen

# COMMAND ----------

!pip install faker

# COMMAND ----------

spark.sql("create database if not exists apj_bootcamp_datasets")
spark.sql("use apj_bootcamp_datasets")

# COMMAND ----------

import random, json
from dateutil import parser

# create epoch for given timezone
def getEpoch(d,h,m, tz = "+13:00"):
  strtime = d + "T" + "{:02d}".format(h) + ":" + "{:02d}".format(m) + ":00" + tz
  epoch = int(parser.parse(strtime).timestamp())
  return epoch

#test output
print(getEpoch('2022-01-12', 14 ,41, "+12:00") )
spark.udf.register("getEpochSQL", getEpoch)

# Generate items for each order
def getOrderItems():
  numberOfItems = random.randint(1,5)
  numberOfCustomIngredients = random.randint(1,8)
  ls = []
  # add more weight to Custom option
  juiceSelection = ["Custom", "Custom","Custom","Custom","Custom","Custom","Custom","Custom","Orange Lake", "Fruit Warehouse", "ACID Sunshine", "SQL Paths","Worth The Squeeze" ,"Fruits Of Labor" ,"Craze" ,"Drink Your Greens" ,"Power Punch" ,"Packed Punch" ,"Powerful Punch" ,"Punch" ,"Joyful" ,"Complete Cleanse" ,"Get Clean" ,"Drink Your Vitamins" ,"Drinkable Vitamins" ,"Healthy Resource" ,"Healthy" ,"Jeneration" ,"Jumpstart" ,"Justified" ,"No Excuse" ,"Jungle" ,"Jumble" ,"Blended Benefits" ,"Squeezed Sweetness" ,"Super Squeezed" ,"Pulp Power" ,"Bounty Of Benefits" ,"Tough And Tasty" ,"Refreshing Reward" ,"Rapid Reward" ,"Fit Drink" ,"Healthy Hydration" ,"Hydration Station" ,"Just Juicy" ,"Juicy Hydration" ,"Nothing To Lose" ,"Indulgent" ,"Fit Fuel"]
  drinkSize = ["Small", "Medium", "Large"]
  allIngredients = ["Apple","Orange","Carrot","Beatroot","Spinach","Lemon","Yogurt","Melon","Blueberry","Cucumber","Tomatoe","Banana","Strawberries","Clementine","Lemon","Ginger","Pineapple","Celery","Spinach"]
  priceList = [5,7,9]
  for x in range(1, numberOfItems+1):
    item = {"id": random.choice(juiceSelection), "size": random.choice(drinkSize), "notes": random.choices(["","no ice","extra sugar","extra ice"], weights=[9,1,1,1])[0], "cost": 0 }
    item["cost"] = priceList[0] if item["size"] == "Small" else (priceList[1] if item["size"] == "Medium" else priceList[2])
    if item["id"] == "Custom":
      customIngredients = []
      for y in range(1,numberOfCustomIngredients):
        customIngredients.append(random.choice(allIngredients))
      item["ingredients"] = customIngredients
      item["cost"] = item["cost"] + 1 # extra for custom juice
    ls.append(item)  
  #print(len(ls))
  return json.dumps(ls)

# test output
getOrderItems()
spark.udf.register("getOrderItemsSQL", getOrderItems)

# COMMAND ----------

import dbldatagen as dg
from datetime import timedelta, datetime
import random, string, uuid
from pyspark.sql.types import IntegerType, FloatType, StringType, LongType, StructType, ArrayType, StructField


def generate_sales_records(sales_date, location, location_timezone, customer_id_list, no_customer_coverage, opening_hours=range(8,18), busy_hours=[4,5,2,2,5,7,4,1,3,1], min_sales = 10, max_sales = 50):
  data_rows = random.randint(min_sales,max_sales) 

  df_spec = (dg.DataGenerator(spark, name="single_day_dataset", rows=data_rows,partitions=8)
                              .withColumn("SaleID", StringType(), expr="uuid()")
                              .withColumn("SaleDate", StringType(), values=[sales_date])
                              .withColumn("SaleHour", IntegerType(), values = opening_hours, random=True, weights=busy_hours)
                              .withColumn("SaleMinute", IntegerType(), minValue=0, maxValue=59, random=True)
                              .withColumn("tz", StringType(), values=[location_timezone])
                              .withColumn("ts", LongType(), expr="getEpochSQL(SaleDate,SaleHour,SaleMinute,tz)", baseColumn=["SaleDate","SaleHour", "SaleMinute","tz"])
                              .withColumn("STATE", StringType(), values=['COMPLETED','CANCELED','PENDING'], random=True, weights=[9,1,1])
                              .withColumn("PaymentMethod", StringType(), values=['CASH','CARD','ONLINE'])
                              .withColumn("OrderSource", StringType(), values=['IN-STORE','ONLINE','WEB-APP'], random = True, weights = [99,5,0])
                              .withColumn("Location", StringType(), values=[location], random=True)
                              .withColumn("CustomerID", StringType(), values= customer_id_list, percentNulls=no_customer_coverage, random=True)
                              .withColumn("SaleItems", StringType(), expr="getOrderItemsSQL()")
                              )
  df = df_spec.build()
  num_rows=df.count()
  #print(f"Returning {num_rows} records for date: {sales_date} and location: {location}")
  return df.drop("SaleDate","SaleHour","SaleMinute","tz")

# test output
tsd_df = generate_sales_records('2022-01-01', 'AKL01', '+13:00', range(1,8), 0.2)
tsd_df.createOrReplaceTempView('tsd_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_json(SaleItems, 'array<struct<id:string,size:string,notes:string,cost:double,ingredients:array<string>>>'), * from tsd_df LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists working_db;
# MAGIC 
# MAGIC drop table if exists working_db.working_table_apj_locations;
# MAGIC 
# MAGIC create table working_db.working_table_apj_locations 
# MAGIC (id string,
# MAGIC store_name string,
# MAGIC location_timezone string,
# MAGIC open_hours string,
# MAGIC busy_hours string,
# MAGIC number_of_customers int,
# MAGIC missing_customers float,
# MAGIC min_sales int,
# MAGIC max_sales int);
# MAGIC 
# MAGIC insert into working_db.working_table_apj_locations
# MAGIC values ('AKL01', 'Auckland Airport', '+13:00', '8-17', '1,2,2,5,7,4,2,3,2,1', 500, 0.8, 100, 5000),
# MAGIC ('AKL02', 'Auckland CBD', '+13:00', '10-17', '2,2,5,7,4,6,6,1', 100, 0.25, 100, 1500),
# MAGIC ('WLG01', 'Wellington CBD', '+13:00', '8-17', '4,5,2,2,5,7,4,1,3,1', 800, 0.3, 100, 2000),
# MAGIC ('SYD01', 'Sydney CBD', '+10:00', '8-19', '4,5,2,2,5,7,4,1,3,5,2,1', 750, 0.5, 2000, 4000),
# MAGIC ('MEL01', 'Melbourne CBD', '+11:00', '8-19', '4,5,2,2,5,7,4,1,3,5,2,1', 850, 0.5, 2000, 4000),
# MAGIC ('MEL02', 'Melbourne Airport', '+11:00', '0-23', '1,1,1,2,1,2,3,6,7,6,3,4,4,2,6,7,4,5,8,8,5,2,1,1', 750, 0.95, 2000, 5000),
# MAGIC ('BNE02', 'Brisbane Airport', '+10:00', '6-19', '2,3,6,7,6,3,4,4,2,6,7,4,5,8', 200, 0.9, 10, 5000),
# MAGIC ('PER01', 'Perth CBD', '+8:00', '6-19', '2,3,6,7,6,3,4,4,2,6,7,4,5,8', 500, 0.50, 100, 3000),
# MAGIC ('CBR01', 'Canberra Airport', '+11:00', '8-19', '4,5,2,2,5,7,4,1,3,5,2,1', 200, 0.7, 10, 2000)

# COMMAND ----------

from datetime import date, timedelta, datetime

spark.sql("drop table if exists apj_juice_sales")
records = spark.table("working_db.working_table_apj_locations").collect()

start_date = date(2021,12,1)

generate_days = 31
for r in records:
  for x in range(0,generate_days+1):
    sale_date =  (start_date + timedelta(days=x)).strftime("%Y-%m-%d")
    daily_df = generate_sales_records(sale_date,\
                                      r.id,\
                                      r.location_timezone,\
                                      range(1,r.number_of_customers + 1),\
                                      r.missing_customers,\
                                      range(int(r.open_hours.split('-')[0]),int(r.open_hours.split('-')[1])+1),\
                                      [int(item) for item in r.busy_hours.split(',')],\
                                      r.min_sales,\
                                      r.max_sales)
    daily_df.write.mode("append").saveAsTable("apj_juice_sales")


# COMMAND ----------

# set up Azure Storage link 

storage_name = "apjbootcamp"
output_container_name = "sales"

sas = ""

spark.conf.set(f"fs.azure.sas.{output_container_name}.{storage_name}.blob.core.windows.net", sas)
#test connection
dbutils.fs.ls(f"wasbs://{output_container_name}@{storage_name}.blob.core.windows.net/")

# COMMAND ----------

all_sales = spark.table("apj_juice_sales")

file_path = f"wasbs://{output_container_name}@{storage_name}.blob.core.windows.net/202112/"
dbutils.fs.rm(file_path, True)
all_sales.coalesce(1).write.format('json').save(file_path)


# COMMAND ----------

# generate dim_users

records = spark.table("working_db.working_table_apj_locations").collect()

from faker import Faker
fake = Faker()

columns= ["store_id", "id", "name", "email"]
df_users = spark.createDataFrame(data = [], schema="store_id:string,id:string,name:string,email:string")

for r in records:
  user_dataset = [(r.id, i, fake.name(), fake.email()) for i in range(1,r.number_of_customers+1)]
  df_users = df_users.union(spark.createDataFrame(data = user_dataset, schema = columns))

df_users.display()

# upload to Azure for now
file_path = f"wasbs://{output_container_name}@{storage_name}.blob.core.windows.net/dim_users/"
dbutils.fs.rm(file_path, True)

df_users \
  .coalesce(1) \
  .write \
  .mode('overwrite') \
  .option('header', 'true') \
  .csv(file_path)

# COMMAND ----------

#generate dim_stores

store_columns= ["id", "name", "email", "city", "hq_address", "phone_number"]
stores_dataset = []

fakeNZ = Faker("en_NZ")
fakeAUS = Faker("en_AU")

for r in records:
  if r.id.startswith('AKL') or r.id.startswith('WLG'):
    stores_dataset.append((r.id, r.store_name, fakeNZ.email(), r.store_name.split(' ')[0], fakeNZ.address(), fakeNZ.phone_number()))
  else:
    stores_dataset.append((r.id, r.store_name, fakeAUS.email(), r.store_name.split(' ')[0], fakeAUS.address(), fakeAUS.phone_number()))

df_stores = spark.createDataFrame(data = stores_dataset, schema = store_columns)
df_stores.display()


# upload to Azure for now
file_path = f"wasbs://{output_container_name}@{storage_name}.blob.core.windows.net/dim_stores/"
dbutils.fs.rm(file_path, True)

df_stores \
  .coalesce(1) \
  .write \
  .mode('overwrite') \
  .option('header', 'true') \
  .csv(file_path)


# COMMAND ----------

def generate_hourly_sales(sale_date, hour, location, output_location):
  attributes = spark.sql(f"SELECT * from working_db.working_table_apj_locations where id = '{location}'").collect()[0]
  
  data_rows = random.randint(attributes.min_sales,attributes.max_sales) / 24 # convert daily sales to hourly
  customer_id_list = range(1, attributes.number_of_customers + 1)

  df_spec = (dg.DataGenerator(spark, name="single_hour_dataset", rows=data_rows,partitions = 4)
                              .withColumn("SaleID", StringType(), expr="uuid()")
                              .withColumn("SaleDate", StringType(), values=[sale_date])
                              .withColumn("SaleHour", IntegerType(), values=[hour])
                              .withColumn("SaleMinute", IntegerType(), minValue=0, maxValue=59, random=True)
                              .withColumn("tz", StringType(), values=[attributes.location_timezone])
                              .withColumn("ts", LongType(), expr="getEpochSQL(SaleDate,SaleHour,SaleMinute,tz)", baseColumn=["SaleDate","SaleHour", "SaleMinute","tz"])
                              .withColumn("STATE", StringType(), values=['COMPLETED','CANCELED','PENDING'], random=True, weights=[9,1,1])
                              .withColumn("PaymentMethod", StringType(), values=['CASH','CARD','ONLINE'])
                              .withColumn("OrderSource", StringType(), values=['IN-STORE','ONLINE','WEB-APP'], random = True, weights = [99,5,0])
                              .withColumn("Location", StringType(), values=[location], random=True)
                              .withColumn("CustomerID", StringType(), values= customer_id_list, percentNulls=attributes.missing_customers, random=True)
                              .withColumn("SaleItems", StringType(), expr="getOrderItemsSQL()")
                              )
  df = df_spec.build()
  num_rows=df.count()
  #print(f"Returning {num_rows} records for date: {sales_date} and location: {location}")
  df.drop("SaleDate","SaleHour","SaleMinute","tz") \
  .coalesce(1) \
  .write \
  .mode('overwrite') \
  .json(output_location)
  


generate_hourly_sales(sale_date = '2022-01-18', hour = 18, location = 'SYD01', output_location = f"wasbs://{output_container_name}@{storage_name}.blob.core.windows.net/autoloader/2022011818")
