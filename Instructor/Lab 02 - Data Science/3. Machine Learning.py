# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Let the Machines Learn! 
# MAGIC <div style="float:right">
# MAGIC   <img src="files/ajmal_aziz/bootcamp_data/machine_learning_model.png" width="1000px">
# MAGIC </div>
# MAGIC 
# MAGIC We are going to train an a model to predict the quality of an orange given the chemical makeup of the orange. This will help us find the key indicators of quality.
# MAGIC The key indicators can then be used engineer a great orange! We will begin as follows:
# MAGIC 
# MAGIC 1. The feature set will then be uploaded to the Feature Store.
# MAGIC 2. We will pre-process our numerical and categorial column(s).
# MAGIC 3. We will train multiple models from our ML runtime and assess the best model.
# MAGIC 4. Machine learning model metrics, parameters, and artefacts will be tracked with mlflow.
# MAGIC 5. 🚀 Deployment of our model!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating a training set from our Feature Store

# COMMAND ----------

# DBTITLE 1,Our original data set does not have our calculated features
DATABASE_NAME = "anz_bootcamp3_ml_db"
spark.sql(f"USE {DATABASE_NAME}")
data = spark.table("experiment_results")
display(data)

# COMMAND ----------

from sklearn.model_selection import train_test_split

train_portion = 0.7
validation_portion = 0.2
testing_portion = 0.1

train_df, test_df = train_test_split(data.toPandas(), test_size=1 - train_portion)
valid_df, test_df = train_test_split(train_df, test_size=testing_portion/(testing_portion + validation_portion))

# Just to make sure the maths worked out ;)
# assert train_df.shape[0]/(training_df.shape[0] testing_df.shape[0] + validation_portion.shape[0])

# COMMAND ----------

# DBTITLE 1,We now use FeatureLookup to pull features from our feature store
from databricks.feature_store import FeatureLookup, FeatureStoreClient

fs = FeatureStoreClient()

feature_table = "feature_store_apjuice.oj_experiment"

feature_lookup = FeatureLookup(
  table_name=feature_table,
  #          Pull only our calculated features
  #             |
  #             |
  feature_names=["h_concentration", "acidity_ratio"],
  lookup_key = ["customer_id"]
)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,We generate a training and testing data set from our feature store
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

testing_set = fs.create_training_set(
  df=spark.createDataFrame(test_df),
  feature_lookups=[feature_lookup],
  label = 'quality',
  exclude_columns="customer_id"
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Partition our data set into training, validation (for tuning), and testing (for reporting accuracy only)
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler

preprocessing_pipeline = []

one_hot_pipeline = Pipeline(steps=[
    ("imputer", SimpleImputer(missing_values=None, strategy="constant", fill_value="")),
    ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessing_pipeline.append(("process_categorical", one_hot_pipeline, categorical_feature_columns))

# COMMAND ----------

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputer", SimpleImputer(strategy="mean")),
    ("scaler", StandardScaler())
])

preprocessing_pipeline.append(("process_numerical", numerical_pipeline, numerical_feature_columns))
