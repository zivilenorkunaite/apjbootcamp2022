# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML Classification Notebook
# MAGIC 
# MAGIC This notebook will go through how to leverage AutoML in order to run a classification experiment.
# MAGIC For more details:
# MAGIC - See (AWS): https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-api.html
# MAGIC - See (Azure): https://learn.microsoft.com/en-au/azure/databricks/machine-learning/automl/train-ml-model-automl-api

# COMMAND ----------

# MAGIC %run "./Utils/Fetch_User_Metadata"

# COMMAND ----------

# Load Raw Data
spark.sql(f"USE {DATABASE_NAME}")
data = spark.table("phytochemicals_quality")

# Train Test Split
from sklearn.model_selection import train_test_split

train_portion = 0.7
valid_portion = 0.2
test_portion = 0.1

train_df, test_df = train_test_split(data.toPandas(), test_size=test_portion)
train_df, valid_df = train_test_split(train_df, test_size= 1 - train_portion/(train_portion+valid_portion))

# Just to make sure the maths worked out ;)
assert round((train_df.shape[0] / (train_df.shape[0] + valid_df.shape[0] + test_df.shape[0])), 2) == train_portion

# COMMAND ----------

# DBTITLE 1,Load Features from the Feature Store
from databricks.feature_store import FeatureLookup, FeatureStoreClient

fs = FeatureStoreClient()

feature_table = f"{DATABASE_NAME}.features_oj_prediction_experiment"

feature_lookup = FeatureLookup(
  table_name=feature_table,
  #
  #          Pull our calculated features from our feature store
  #             |
  #             |
  feature_names=["h_concentration", "acidity_ratio"],
  lookup_key = ["customer_id"]
)

# COMMAND ----------

# DBTITLE 1,Create Training and Testing Sets
training_set = fs.create_training_set(
  df=spark.createDataFrame(train_df),
  feature_lookups=[feature_lookup],
  label = 'quality',
  exclude_columns="customer_id"
)

validation_set = fs.create_training_set(
  df=spark.createDataFrame(valid_df),
  feature_lookups=[feature_lookup],
  label = 'quality',
  exclude_columns="customer_id"
)

training_data = training_set.load_df().toPandas()
validation_data = validation_set.load_df().toPandas()

# COMMAND ----------

# DBTITLE 1,Run AutoML
from databricks import automl
summary = automl.classify(training_data, target_col="quality", timeout_minutes=5, experiment_name="AP Juice AutoML Expr")

# COMMAND ----------


