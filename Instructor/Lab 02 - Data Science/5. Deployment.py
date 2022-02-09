# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Model Serving
# MAGIC <div style="float:right">
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/06/Three-Principles-for-Selecting-Machine-Learning-Platforms-blog-img-1.jpg" width=1000>
# MAGIC </div>
# MAGIC 
# MAGIC Let's turn our attention to the model deployment and serving aspect of the platform.

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
# MAGIC <div style="float:right">
# MAGIC <img src="https://databricks.com//wp-content/uploads/2020/06/blog-mlflow-model-3.gif" >
# MAGIC </div>

# COMMAND ----------


