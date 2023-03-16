# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Let the Machines Learn! 
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://ajmal-field-demo.s3.ap-southeast-2.amazonaws.com/apj-sa-bootcamp/machine_learning_model.png" width="1000px">
# MAGIC </div>
# MAGIC 
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

# MAGIC %run "./Utils/Fetch_User_Metadata"

# COMMAND ----------

# DBTITLE 1,Note: our original data set does not have our calculated features
spark.sql(f"USE {DATABASE_NAME}")
data = spark.table("phytochemicals_quality")
display(data)

# COMMAND ----------

# DBTITLE 1,First: split our raw data into training, validation (for tuning), and test (for reporting purposes only)
from sklearn.model_selection import train_test_split

train_portion = 0.7
valid_portion = 0.2
test_portion = 0.1

train_df, test_df = train_test_split(data.toPandas(), test_size=test_portion)
train_df, valid_df = train_test_split(train_df, test_size= 1 - train_portion/(train_portion+valid_portion))

# Just to make sure the maths worked out ;)
assert round((train_df.shape[0] / (train_df.shape[0] + valid_df.shape[0] + test_df.shape[0])), 2) == train_portion

# COMMAND ----------

# DBTITLE 1,We need a FeatureLookup to pull our custom features from our feature store
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

# DBTITLE 1,We generate a training and validation data set using our feature lookups
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

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Machine Learning Pipeline
# MAGIC We will apply different transformations for numerical and categorical columns. 
# MAGIC 
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://www.pngkit.com/png/full/37-376558_pipe-8-bit-mario.png" width="800px">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC For numerical columns, we will:
# MAGIC - Impute missing values with the mean of the column.
# MAGIC - Scale numerical values to 0 mean and unit variance to reduce the impact of larger values.
# MAGIC 
# MAGIC For categorical columns:
# MAGIC - Impute missing values with empty strings
# MAGIC - One hot encode the type of orange

# COMMAND ----------

# DBTITLE 1,Tiny bit of cleanup before we define our pipeline 🧹 
# Gather and exogenous and endogenous variables from training and validation
X_training = training_data.drop(columns=['quality'], axis=1)
y_training = (training_data['quality'] == "Good").astype(int)

# MLflow 1.x required validation in this format
X_validation = validation_data.drop(columns=['quality'], axis=1)
y_validation = (validation_data['quality'] == "Good").astype(int)

# MLFLow API change in 2.x means we need the validation as a single data table
validation_dataset = validation_data
validation_dataset['quality'] = (validation_data['quality'] == "Good").astype(int)

# Gather the numerical and categorical features
numerical_features = X_training._get_numeric_data().columns.tolist()
categorical_features = list(set(X_training.columns) - set(numerical_features))

# COMMAND ----------

# DBTITLE 1,💾 Save for future use
# Save into our database for future use
def save_to_db(df, name):
  (spark.createDataFrame(df)
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(f"{DATABASE_NAME}.{name}")
  )

save_to_db(X_validation, "X_validation")
save_to_db(X_training, "X_training")

# COMMAND ----------

# DBTITLE 1,We define our processing pipeline, starting with our categorical columns
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler

preprocessing_pipeline = []

one_hot_pipeline = Pipeline(steps=[
    ("imputer", SimpleImputer(missing_values=None, strategy="constant", fill_value="")),
    ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessing_pipeline.append(("process_categorical", one_hot_pipeline, categorical_features))

# COMMAND ----------

# DBTITLE 1,Numerical column transformers
numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputer", SimpleImputer(strategy="mean")),
    ("scaler", StandardScaler())
])

preprocessing_pipeline.append(("process_numerical", numerical_pipeline, numerical_features))

# COMMAND ----------

# DBTITLE 1,Putting our transformers together
from sklearn.compose import ColumnTransformer

preprocessor = ColumnTransformer(preprocessing_pipeline, remainder="passthrough", sparse_threshold=0)

# COMMAND ----------

# DBTITLE 1,Our pipeline needs to end in an estimator, we will use a random forrest at first
from sklearn.ensemble import RandomForestClassifier

rf_params = {
  'bootstrap': False,
  'criterion': "entropy",
  'min_samples_leaf': 50,
  'min_samples_split': 100
}

classifier = RandomForestClassifier(**rf_params)

# COMMAND ----------

# DBTITLE 1,We can now inspect our modelling pipeline
from sklearn import set_config
from sklearn.pipeline import Pipeline

set_config(display="diagram")

rf_model = Pipeline([
    ("preprocessor", preprocessor),
    ("classifier", classifier),
])

rf_model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks uses mlflow for experiment tracking, logging, and production
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2020/06/blog-mlflow-model-1.png" width="1000px">
# MAGIC </div>
# MAGIC 
# MAGIC We are going to use the sklearn 'flavour' in mlflow. A couple of things to note about Mlflow:
# MAGIC 
# MAGIC - Mlflow is going to help orchestrate the end-to-end process for our machine learning use-case.
# MAGIC - **runs**: MLflow Tracking is organized around the concept of runs, which are executions of some piece of data science code.
# MAGIC - **experiments**: MLflow allows you to group runs under experiments, which can be useful for comparing runs intended to tackle a particular task.
# MAGIC 
# MAGIC 👇 We are going to create an experiment to store our machine learning model runs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Create an Experiment Manually 👩‍🔬
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://ajmal-field-demo.s3.ap-southeast-2.amazonaws.com/apj-sa-bootcamp/create_experiment.gif" width="800px">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC We are now going to manually create an experiment using our UI. To do this, we will follow the following steps:
# MAGIC 
# MAGIC 0. Ensure that you are in the Machine Learning persona by checking the LHS pane and ensuring it says **Machine Learning**.
# MAGIC - Click on the ```Experiments``` button.
# MAGIC - Click the "Create an AutoML Experiment" arrow dropdown
# MAGIC - Press on **Create Blank Experiment**
# MAGIC - Put the experiment name as: "**first_name last_name Orange Quality Prediction**", so e.g. "Ajmal Aziz Orange Quality Prediction"

# COMMAND ----------

# DBTITLE 1,We create a blank experiment to log our runs to
experiment_id = <>

# For future reference, of course, you can use the mlflow APIs to create and set the experiment

# experiment_name = "Orange Quality Prediction"
# experiment_path = os.path.join(PROJECT_PATH, experiment_name)
# experiment_id = mlflow.create_experiment(experiment_path)

# mlflow.set_experiment(experiment_path)

# COMMAND ----------

# DBTITLE 1,We use mlflow's sklearn flavour to log and evaluate our model as an experiment run
import mlflow
import pandas as pd
from mlflow.models.signature import infer_signature

# Enable automatic logging of input samples, metrics, parameters, model signtures and models
mlflow.sklearn.autolog(log_input_examples=True, log_model_signatures=True, log_models=True)

with mlflow.start_run(run_name="random_forest_pipeline",
                      experiment_id=experiment_id) as mlflow_run:
    # Fit our estimator
    rf_model.fit(X_training, y_training)
    
    # Log our parameters
    mlflow.log_params(rf_params)
    
    # This method was deprecated from MLflow 2.x and removed
    # Training metrics are logged by MLflow autologging
    # Log metrics for the validation set
    #mlflow.sklearn.eval_and_log_metrics(rf_model,
    #                                    X_validation,
    #                                    y_validation,
    #                                    prefix="val_")

    # From MLflow 2.x use this to log validation metrics

    # Log the model first and extract the uri
    #mlflow.sklearn.log_model(rf_model, "model")
    model_uri = mlflow.get_artifact_uri("model")

    # Evaluate and log the model results
    results = mlflow.evaluate(model=model_uri, data=validation_dataset, 
                              targets='quality', model_type="classifier",
                             evaluator_config = {'metric_prefix':"val_"})
    
    # Need to log model signatures, otherwise it will fail when using spark_udf for the distributed inference
    signature = infer_signature(X_training, rf_model.predict(X_training))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://ajmal-field-demo.s3.ap-southeast-2.amazonaws.com/apj-sa-bootcamp/shap_logged.gif" width="600px">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC ## Logging other artefacts in runs
# MAGIC 
# MAGIC We have flexibility over the artefacts we want to log. By logging artefacts with runs we have examine the quality of fit to better determine if we have overfit or if we need to retrain, etc. These artefacts also help with reproducibility.
# MAGIC 
# MAGIC As an example, let's log the partial dependence plot from SHAP with a single model run. 👇

# COMMAND ----------

def generate_shap_plot(model, data):
  import shap
  global image
  sample_data = data.sample(n=100)
  explainer = shap.TreeExplainer(model["classifier"])
  shap_values = explainer.shap_values(model["preprocessor"].transform(sample_data))
  
  fig = plt.figure(1)
  ax = plt.gca()
  
  shap.dependence_plot("rank(1)", shap_values[0],
                       model["preprocessor"].transform(sample_data),
                       ax=ax, show=False)
  plt.title(f"Acidity dependence plot")
  plt.ylabel(f"SHAP value for the Acidity")
  image = fig
  # Save figure
  fig.savefig(f"/dbfs/FileStore/{USERNAME}_shap_plot.png")

  # Close plot
  plt.close(fig)
  return image

# COMMAND ----------

# DBTITLE 1,We now log our SHAP image as an artefact within this run
# Enable automatic logging of input samples, metrics, parameters, and models
import matplotlib.pyplot as plt

with mlflow.start_run(run_name="random_forest_pipeline_2", experiment_id=experiment_id) as mlflow_run:
    # Fit our estimator
    rf_model.fit(X_training, y_training)
    
    # Log our parameters
    mlflow.log_params(rf_params)
    
    # This method was deprecated from MLflow 2.x and removed
    # Training metrics are logged by MLflow autologging
    # Log metrics for the validation set
    #mlflow.sklearn.eval_and_log_metrics(rf_model,
    #                                    X_validation,
    #                                    y_validation,
    #                                    prefix="val_")
    
    # From MLflow 2.x use this to log validation metrics

    # Log the model first and extract the uri
    #mlflow.sklearn.log_model(rf_model, "model")
    model_uri = mlflow.get_artifact_uri("model")

    # Evaluate and log the model results
    results = mlflow.evaluate(model=model_uri, data=validation_dataset, 
                              targets='quality', model_type="classifier",
                             evaluator_config = {'metric_prefix':"val_"})

    shap_fig = generate_shap_plot(rf_model, X_validation)
    mlflow.log_artifact(f"/dbfs/FileStore/{USERNAME}_shap_plot.png")
    
    # Need to log model signatures, otherwise it will fail when using spark_udf for the distributed inference
    signature = infer_signature(X_training, rf_model.predict(X_training))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Search best hyper parameters with HyperOpt (Bayesian optimization) accross multiple nodes
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/bayesian-model.png" style="height: 330px"/></div>
# MAGIC Our model performs well but we want to run hyperparameter optimisation across our parameter search space. We will use HyperOpt to do so. For fun, we're going to try an XGBoost model here.
# MAGIC 
# MAGIC This model is a good start, but now we want to try multiple hyper-parameters and search the space rather than a fixed size.
# MAGIC 
# MAGIC GridSearch could be a good way to do it, but not very efficient when the parameter dimension increase and the model is getting slow to train due to a massive amount of data.
# MAGIC 
# MAGIC HyperOpt search accross your parameter space for the minimum loss of your model, using Baysian optimization instead of a random walk

# COMMAND ----------

# MAGIC %md
# MAGIC ![my_test_image](https://www.jeremyjordan.me/content/images/2017/11/grid_search.gif)
# MAGIC ![my_test_image](https://www.jeremyjordan.me/content/images/2017/11/Bayesian_optimization.gif)

# COMMAND ----------

from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope


search_space = {
  'max_depth': scope.int(hp.quniform('max_depth', 4, 100, 1)),
  'colsample_bytree': hp.uniform('colsample_bytree', 0.01, 1), 
  'learning_rate': hp.loguniform('learning_rate', 0.01, 1),
  'n_estimators': scope.int(hp.quniform('n_estimators', 100, 1000, 10)),
  'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
  'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
  'min_child_weight': hp.loguniform('min_child_weight', -1, 6),
  'objective': 'binary:logistic',
  'verbosity': 0,
  'n_jobs': -1
}

# COMMAND ----------

# we need to fix the experiment for the nesting to work properly
mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

# DBTITLE 1,We begin by defining our objective function
# With MLflow autologging, hyperparameters and the trained model are automatically logged to MLflow.
from mlflow.models.signature import infer_signature
from xgboost import XGBClassifier
import mlflow.xgboost
from sklearn.metrics import roc_auc_score


def f_train(trial_params):
  with mlflow.start_run(nested=True):
    mlflow.xgboost.autolog(log_input_examples=True, silent=True)
    
    # Redefine our pipeline with the new params from the search algo    
    classifier = XGBClassifier(**trial_params)
    
    xgb_model = Pipeline([
      ("preprocessor", preprocessor),
      ("classifier", classifier)
    ])
    
    # Fit, predict, score
    xgb_model.fit(X_training, y_training)
    predictions_valid = xgb_model.predict(X_validation)
    auc_score = roc_auc_score(y_validation.values, predictions_valid)
    
    # Log :) 
    signature = infer_signature(X_training, xgb_model.predict(X_training))
    mlflow.log_metric('auc', auc_score)

    # Set the loss to -1*auc_score so fmin maximizes the auc_score
    return {'status': STATUS_OK, 'loss': -1*auc_score}

# COMMAND ----------

# Greater parallelism will lead to speedups, but a less optimal hyperparameter sweep. 
# A reasonable value for parallelism is the square root of max_evals.
spark_trials = SparkTrials(parallelism=10)

# Run fmin within an MLflow run context so that each hyperparameter configuration is logged as a child run of a parent
with mlflow.start_run(run_name='xgboost_models') as mlflow_run:
  best_params = fmin(
    fn=f_train, 
    space=search_space, 
    algo=tpe.suggest, 
    max_evals=5,
    trials=spark_trials, 
  )

# COMMAND ----------


