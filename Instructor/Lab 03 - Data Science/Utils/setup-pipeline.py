# Databricks notebook source
# MAGIC %run "./Utils/Fetch_User_Metadata"

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")
data = spark.table("phytochemicals_quality")
display(data)

# COMMAND ----------

from databricks.feature_store import FeatureLookup, FeatureStoreClient

fs = FeatureStoreClient()

feature_table = f"{DATABASE_NAME}.features_oj_experiment"

feature_lookup = FeatureLookup(
  table_name=feature_table,
  #
  #          Pull our calculated features from our feature store
  #             |
  #             |
  feature_names=["h_concentration", "acidity_ratio"],
  lookup_key = ["customer_id"]
)
