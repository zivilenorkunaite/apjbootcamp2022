# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Model Serving
# MAGIC 
# MAGIC Let's turn our attention to the model deployment and serving aspect of the platform.
# MAGIC 
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/06/Three-Principles-for-Selecting-Machine-Learning-Platforms-blog-img-1.jpg" width=1000>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Grabbing your user credentials 👩‍💻
# MAGIC %run "./Utils/Fetch_User_Metadata"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's manually grab our Experiment ID from earlier
# MAGIC 
# MAGIC Steps: 
# MAGIC 1. Click Experiments on the LHS of the pane
# MAGIC - Navigate to the notebook where the runs are recorded from earlier
# MAGIC - Copy the Experiment ID from the top of the page and populate below 👇 

# COMMAND ----------

experiment_id = 3530875234215041

# Alternatively, 
# import mlflow

# experiment_name = 'Ajmal Aziz Orange Quality Prediction'
# experiment_path = os.path.join(PROJECT_PATH, experiment_name)
# experiment_id = mlflow.tracking.MlflowClient().get_experiment_by_name(experiment_path).experiment_id

# COMMAND ----------

# DBTITLE 1,We can look at the runs we went through whilst training our models
import mlflow 

all_runs = mlflow.search_runs(experiment_ids=[experiment_id])
all_runs

# COMMAND ----------

# DBTITLE 1,Let's pick out our best run by order by the validation roc auc score
from pprint import pprint

best_run = mlflow.search_runs(
  experiment_ids=[experiment_id],
  filter_string='metrics.val_roc_auc_score > 0.5',
  order_by=['metrics.val_roc_auc_score desc']
).iloc[0]

pprint(best_run)

# COMMAND ----------

# DBTITLE 1,Let's start by registering our model
from mlflow.tracking import MlflowClient

client = MlflowClient()

model_name = f'orange_experiment_{USERNAME}'
print(f'Will be using model name: "{model_name}"')

client.create_registered_model(model_name)

displayHTML(f"<h2>Check the model at <a href='#mlflow/models/{model_name}'>#mlflow/models/{model_name}</a></h2>")

# COMMAND ----------

# DBTITLE 1,We can register the best model in our model registry for the rest of the team to see
mlflow.register_model(f'runs:/{best_run["run_id"]}/model', model_name)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,We can use the mlflow API (or do this manually) to transition our model to production.
import mlflow

client = mlflow.tracking.MlflowClient()

model_name = 'Pytorch Model'
model_version = 1
registered_model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")

# #                                                                               New stage
# #                                                    Previous version           |
# #                                                         |                     |
# client.transition_model_version_stage(model_name, model_version, stage="Production", archive_existing_versions=True)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Inference
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC <img src="https://databricks.com//wp-content/uploads/2020/06/blog-mlflow-model-3.gif" >
# MAGIC </div>

# COMMAND ----------


