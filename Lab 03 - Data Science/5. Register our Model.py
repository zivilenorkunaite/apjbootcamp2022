# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Model Serving and Model Registry
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-4.png" width="1200">
# MAGIC 
# MAGIC One of the primary challenges among data scientists and ML engineers is the absence of a central repository for models, their versions, and the means to manage them throughout their lifecycle.  
# MAGIC 
# MAGIC [The MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) addresses this challenge and enables members of the data team to:
# MAGIC <br><br>
# MAGIC * **Discover** registered models, current stage in model development, experiment runs, and associated code with a registered model
# MAGIC * **Transition** models to different stages of their lifecycle
# MAGIC * **Deploy** different versions of a registered model in different stages, offering MLOps engineers ability to deploy and conduct testing of different model versions
# MAGIC * **Test** models in an automated fashion
# MAGIC * **Document** models throughout their lifecycle
# MAGIC * **Secure** access and permission for model registrations, transitions or modifications

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
# MAGIC 
# MAGIC > Note: we could also easily use mlflow APIs to do this programmatically

# COMMAND ----------

experiment_id = <>

# COMMAND ----------

# DBTITLE 1,We can look at the runs we went through whilst training our models
import mlflow 

all_runs = mlflow.search_runs(experiment_ids=[experiment_id])
all_runs.head()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### How to Use the Model Registry
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://ajmal-field-demo.s3.ap-southeast-2.amazonaws.com/apj-sa-bootcamp/model_registry.png" width="1000px">
# MAGIC </div>
# MAGIC 
# MAGIC Typically, data scientists who use MLflow will conduct many experiments, each with a number of runs that track and log metrics and parameters. During the course of this development cycle, they will select the best run within an experiment and register its model with the registry.  Think of this as **committing** the model to the registry, much as you would commit code to a version control system.  
# MAGIC 
# MAGIC The registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages.

# COMMAND ----------

# DBTITLE 1,🤩 Let's pick out our best run by order by the validation roc auc score 
best_run = mlflow.search_runs(
  experiment_ids=[experiment_id],
  filter_string="status = 'FINISHED'",
  order_by=["metrics.val_roc_auc_score desc"],
  max_results=1
)

best_run

# COMMAND ----------

# DBTITLE 1,🥺 Similarly, let's pick our worse performing model out of the heap 
worst_run = mlflow.search_runs(
  experiment_ids=[experiment_id],
  filter_string="status = 'FINISHED'",
  order_by=["metrics.val_roc_auc_score asc"],
  max_results=1
)

worst_run

# COMMAND ----------

# DBTITLE 1,Let's begin by registering a model with a unique model name in the model registry
from mlflow.tracking import MlflowClient

client = MlflowClient()

model_name = f'orange_experiment_{USERNAME}'
print(f'Will be using model name: "{model_name}"')

client.create_registered_model(model_name)

displayHTML(f"<h4>Check the model at <a href='#mlflow/models/{model_name}'>#mlflow/models/{model_name}</a></h4>")

# COMMAND ----------

# DBTITLE 1,We now register the worse performing model as version 1
mlflow.register_model(f'runs:/{worst_run.iloc[0]["run_id"]}/model', model_name)

displayHTML(f"<h4>This model will by default be in the version None stage. Check the model at <a href='#mlflow/models/{model_name}'>#mlflow/models/{model_name}</a></h4>")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Let's now promote our model to production
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC   <img src="https://ajmal-field-demo.s3.ap-southeast-2.amazonaws.com/apj-sa-bootcamp/promote_model.gif" width="700px">
# MAGIC </div>
# MAGIC 
# MAGIC Now go ahead and observe the model in the Model Registry:
# MAGIC 0. Ensure that you are in the Machine Learning persona on the LHS
# MAGIC - Click "Models" on the left sidebar
# MAGIC - Find your Model (if your username is **```"yan_moiseev"```**, you should see it as **````orange_experiment_yan_moiseev````**)
# MAGIC - Click on "Version 2"
# MAGIC - Click on "Stage", transition it to "Production"

# COMMAND ----------

# DBTITLE 1,We can register the best model in our model registry for the rest of the team to see
model_registered = mlflow.register_model(f'runs:/{best_run.iloc[0]["run_id"]}/model', model_name)

displayHTML(f"<h4>Check out version 2 of this model at <a href='#mlflow/models/{model_name}'>#mlflow/models/{model_name}</a></h4>")

# COMMAND ----------

# We can also use the mlflow client to make this transition
#                                                                             new stage
#                                                        current version       |
#                                                           |                  |
client.transition_model_version_stage(model_name, model_registered.version, stage="Production", archive_existing_versions=True)

# COMMAND ----------

# client.transition_model_version_stage(name=model_name, version=2, stage='Production')
# client.transition_model_version_stage(name=model_name, version=1, stage='Archived')

# COMMAND ----------

production_model = client.get_latest_versions(model_name, ['Production'])[0]
model = mlflow.sklearn.load_model(production_model.source)

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

# DBTITLE 1,Now we can generate predictions from the production version of the model
spark.sql(f"USE {DATABASE_NAME}")
valid_df = spark.read.table("X_validation").toPandas()
valid_df['quality_predictions'] = model.predict(valid_df)
display(valid_df)

# COMMAND ----------

# DBTITLE 1,We can register our model as spark UDF for distributed inference
spark.udf.register(
  'quality_prediction_model', 
  mlflow.pyfunc.spark_udf(spark, model_uri=production_model.source)
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *, quality_prediction_model(*) as quality_predictions from X_validation

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Let's now register this model for real time inference
# MAGIC <br>
# MAGIC <div style="float:right">
# MAGIC <img src="https://databricks.com//wp-content/uploads/2020/06/blog-mlflow-model-3.gif" >
# MAGIC </div>
