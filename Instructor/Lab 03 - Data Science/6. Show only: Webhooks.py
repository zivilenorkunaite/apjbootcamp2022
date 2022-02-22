# Databricks notebook source
# MAGIC %md
# MAGIC ## Model Registry Webhooks
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-3.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What is a webhook?
# MAGIC A webhook will call a web URL whenever a given event occurs. In our case, that's when a model is updated or deployed.
# MAGIC 
# MAGIC __A primary goal with MLOps is to introduce more robust testing and automation into how we deploy machine learning models.__  
# MAGIC 
# MAGIC To aid in this effort, the MLflow Model Registry supports webhooks that are triggered during the following events in the lifecycle of a model. This will allow us to automatically:
# MAGIC 
# MAGIC 
# MAGIC * Perform general validation checks and tests on any model added to the Registry
# MAGIC * Send notification / slack alert when a new model is updated
# MAGIC * Introducing a new model to acccept traffic for A/B testing
# MAGIC * ...
# MAGIC 
# MAGIC 
# MAGIC *Note that we only have to do this once for our Orange Prediction model.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Webhook Supported events
# MAGIC * A new model is added to the Registry
# MAGIC * A new version of a registered model is added to the Registry
# MAGIC * A model lifecycle transition request is made (e.g., from _Production_ to _Archived_)
# MAGIC * A transition request is accepted or rejected
# MAGIC * A comment is made on a model version

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example
# MAGIC 
# MAGIC In the following example, we have two notebooks - the first commits the model to the Model Registry, and the second runs a series of general validation checks and tests on it. You can see the entire workflow illustrated below.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook.png" width=1000 >
# MAGIC <br><br>
# MAGIC Let's look at how this workflow plays out chronologically:<br><br>
# MAGIC 
# MAGIC 1. Data Scientist finishes model training and commits best model to Registry
# MAGIC 2. Data Scientist requests lifecyle transition of best model to _Staging_
# MAGIC 3. Webhooks are set for transition request event, and trigger a Databricks Job to test the model, and a Slack message to let the organization know that the lifecycle event is occurring
# MAGIC 4. The testing job is launched
# MAGIC 5. Depending on testing results, the lifecycle transition request is accepted or rejected
# MAGIC 6. Webhooks trigger and send another Slack message to report the results of testing

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create Webhooks
# MAGIC 
# MAGIC Setting up webhooks is simple using the Databricks REST API.  There are some helper functions in the `./_resources/API_Helpers` notebook, so if you want to see additional details you can check there.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model testing - Staging transition request
# MAGIC 
# MAGIC A testing notebook has been created by the ML Engineer team (we'll cover that in details soon).
# MAGIC 
# MAGIC To accept the STAGING request, we'll run this notebook as a Databricks Job whenever we receive a request to move a model to STAGING.
# MAGIC 
# MAGIC The job will be in charge to validate or reject the transition upon completion.

# COMMAND ----------

# DBTITLE 1,You can use the jobs API to programmatically create the staging job
# The job is programatically created if it doesn't exist
job_id = get_model_staging_job_id()

#This should be run once. For the demo We'll reset other webhooks to prevent from duplicated call
reset_webhooks(model_name = f"orange_experiment_{USERNAME}")

#Once we have the id of the job running the tests, we add the hook:
create_job_webhook(model_name = f"orange_experiment_{USERNAME}", job_id = job_id)

# COMMAND ----------

import urllib 
import json 
import requests, json

def create_notification_webhook(model_name, slack_url):
  
  trigger_slack = json.dumps({
  "model_name": model_name,
  "events": ["TRANSITION_REQUEST_CREATED"],
  "description": "Notify the MLOps team that a model is requested to move to staging.",
  "status": "ACTIVE",
  "http_url_spec": {
    "url": slack_url
  }
  })
  response = mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_slack)
  return(response)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Notification
# MAGIC We also want to send slack notification when the a model change from one stage to another:

# COMMAND ----------

create_notification_webhook(model_name = f"orange_experiment_{USERNAME}", slack_url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX")
