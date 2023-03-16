# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Delta Live Tables
# MAGIC 
# MAGIC A simple way to build and manage data pipelines for fresh, high quality data! There are 6 key value adds to Delta Live Tables
# MAGIC 
# MAGIC 1. **Continuous or schedule data ingestion:** Leveraging autoloader with schema evolution.
# MAGIC 2. **Declarative ETL pipelines:** Manage the ETL lifecycle with both Python and SQL APIs 
# MAGIC 3. **Change data capture:** Incremental processing with streaming first architectures
# MAGIC 4. **Data quality:** Data quality and validation reports
# MAGIC 5. **Data pipeline observability:**  Data lineage diagram and granular logging for operational, governance, quality, and status of data pipeline.
# MAGIC 6. **Automated scaling and fault tolerance:** Doing the resource management for you, automaticallly scaling clusters with user defined limits.
# MAGIC 7. **Automatic deployment and operations:** Resource management 
# MAGIC 8. **Orchestrate pipelines and worklflows:** Turnkey orchestration mitigating the need for external orchestration
# MAGIC 
# MAGIC 
# MAGIC <div><img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-pipeline.png"/></div>

# COMMAND ----------

# Your database name is your <first-name>_<last-name>_ap_juice_db
USERNAME = ""
DATABASE_NAME = f"{USERNAME}_ap_juice_db"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import mlflow
from mlflow.tracking import MlflowClient
import dlt

# COMMAND ----------

# DBTITLE 1,Let's define our model
client = MlflowClient()

model_name = f'orange_experiment_{USERNAME}'

production_model = client.get_latest_versions(model_name, ['Production'])[0]

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=production_model.source)

# COMMAND ----------

# DBTITLE 1,To ensure that our schema matches, we extract the model features
model_features = loaded_model.metadata.get_input_schema().input_names()

# COMMAND ----------

@dlt.create_table(
  comment="We extract the training data set from our database",  
  table_properties={
    "quality": "gold"
  }    
)
def get_training_data():
  return spark.read.table(f"{DATABASE_NAME}.X_training")

# COMMAND ----------

@dlt.create_table(
  comment="We also extract the validation data set from our database",  
  table_properties={
    "quality": "gold"
  }    
)
def get_validation_data():
  return spark.read.table(f"{DATABASE_NAME}.X_validation")

# COMMAND ----------

@dlt.create_table(
  comment="Inference performed on the training data",  
  table_properties={
    "quality": "gold"
  }    
)
def predict_training_data():
  return dlt.read("get_training_data").withColumn("quality_predictions", loaded_model(*model_features))

# COMMAND ----------

@dlt.create_table(
  comment="Inference performed on the validation data",  
  table_properties={
    "quality": "gold"
  }    
)
def predict_validation_data():
  return dlt.read("get_validation_data").withColumn("quality_predictions", loaded_model(*model_features))

# COMMAND ----------

@dlt.create_table(
  comment="Both tables merged into one.",  
  table_properties={
    "quality": "gold"
  }    
)
def aggregate_datasets():
  return dlt.read("predict_training_data").union(dlt.read("predict_validation_data"))
