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
# MAGIC Webhooks are available through the Databricks REST API or the Python client databricks-registry-webhooks on PyPI.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Types of webhooks
# MAGIC 
# MAGIC There are two types of webhooks based on their trigger targets:
# MAGIC 
# MAGIC * Webhooks with HTTP endpoints (HTTP registry webhooks): Send triggers to an HTTP endpoint.
# MAGIC 
# MAGIC * Webhooks with job triggers (job registry webhooks): Trigger a job in a Databricks workspace.
# MAGIC 
# MAGIC There are also two types of webhooks based on their scope, with different access control requirements:
# MAGIC 
# MAGIC * Model-specific webhooks: The webhook applies to a specific registered model. You must have Can Manage permissions on the registered model to create, modify, delete, or test model-specific webhooks.
# MAGIC 
# MAGIC * Registry-wide webhooks: The webhook is triggered by events on any registered model in the workspace, including the creation of a new registered model. To create a registry-wide webhook, omit the model_name field on creation. You must have workspace admin permissions to create, modify, delete, or test registry-wide webhooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Webhook Supported events
# MAGIC * MODEL_VERSION_CREATED: A new model version was created for the associated model.
# MAGIC 
# MAGIC * MODEL_VERSION_TRANSITIONED_STAGE: A model version’s stage was changed.
# MAGIC 
# MAGIC * TRANSITION_REQUEST_CREATED: A user requested a model version’s stage be transitioned.
# MAGIC 
# MAGIC * COMMENT_CREATED: A user wrote a comment on a registered model.
# MAGIC 
# MAGIC * REGISTERED_MODEL_CREATED: A new registered model was created. This event type can only be specified for a registry-wide webhook, which can be created by not specifying a model name in the create request.
# MAGIC 
# MAGIC * MODEL_VERSION_TAG_SET: A user set a tag on the model version.
# MAGIC 
# MAGIC * MODEL_VERSION_TRANSITIONED_TO_STAGING: A model version was transitioned to staging.
# MAGIC 
# MAGIC * MODEL_VERSION_TRANSITIONED_TO_PRODUCTION: A model version was transitioned to production.
# MAGIC 
# MAGIC * MODEL_VERSION_TRANSITIONED_TO_ARCHIVED: A model version was archived.
# MAGIC 
# MAGIC * TRANSITION_REQUEST_TO_STAGING_CREATED: A user requested a model version be transitioned to staging.
# MAGIC 
# MAGIC * TRANSITION_REQUEST_TO_PRODUCTION_CREATED: A user requested a model version be transitioned to production.
# MAGIC 
# MAGIC * TRANSITION_REQUEST_TO_ARCHIVED_CREATED: A user requested a model version be archived.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example
# MAGIC 
# MAGIC In the following example, we will demo who to use webhooks python client to create a job registry webhook.

# COMMAND ----------

# MAGIC %pip install databricks-registry-webhooks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a test job, we will use webhook to trigger it

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

import json
import requests

# COMMAND ----------

DATABRICKS_DOMAIN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

JOB_API_ENDPOINT = f"https://{DATABRICKS_DOMAIN}/api/2.0/jobs"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# COMMAND ----------

def create_databricks_job(job_name, notebook_path, cluster_settings):
    job_config = {
        "name": job_name,
        "new_cluster": cluster_settings,
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "GIT"
        },
        "git_source": {
            "git_url": "https://github.com/zivilenorkunaite/apjbootcamp2022", #TODO: Change it to the official bootcamp git repo
            "git_provider": "gitHub",
            "git_branch": "main"
        }
    }
    
    response = requests.post(
        f"{JOB_API_ENDPOINT}/create",
        headers=headers,
        data=json.dumps(job_config)
    )
    
    if response.status_code == 200:
        print(f"Job created successfully: {response.json()['job_id']}")
        return response.json()
    else:
        print(f"Error creating job: {response.status_code} - {response.text}")
        return None

# COMMAND ----------

job_name = "Test Job using git notebook"

# Change to your repo
notebook_path = "Lab 03 - Data Science/Utils/test_job" 

# Customize the cluster settings according to your requirements
cluster_settings = {
    "spark_version": "12.2.x-scala2.12",
    "node_type_id": "i3.xlarge",#For AWS workspace, change it to i3.xlarge, for Azure, change it to Standard_DS3_v2
    "num_workers": 1
}

job = create_databricks_job(job_name, notebook_path, cluster_settings)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create webhooks, test them, and confirm their presence in the list of all webhooks

# COMMAND ----------

## SETUP: Fill in variables
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
model_name = 'orange_experiment_yang_wang'
job_id = job['job_id'] 

# COMMAND ----------

from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec, HttpUrlSpec

# COMMAND ----------

# Create a Job webhook
job_spec = JobSpec(job_id=job_id, access_token=access_token)
job_webhook = RegistryWebhooksClient().create_webhook(
  events=["TRANSITION_REQUEST_CREATED"],
  job_spec=job_spec,
  model_name=model_name
)
job_webhook

# COMMAND ----------

# Test the Job webhook
RegistryWebhooksClient().test_webhook(id=job_webhook.id)

# COMMAND ----------

# List all webhooks and verify webhooks just created are shown in the list
webhooks_list = RegistryWebhooksClient().list_webhooks()
for webhook in webhooks_list:
  if webhook.id == job_webhook.id:
    print(webhook)
assert job_webhook.id in [w.id for w in webhooks_list]

# COMMAND ----------

# MAGIC %md #### Create a transition request to trigger webhooks and then clean up webhooks

# COMMAND ----------

import mlflow
from mlflow.utils.rest_utils import http_request
import json

def client():
  return mlflow.tracking.client.MlflowClient()
 
host_creds = client()._tracking_client.store.get_host_creds()

def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()

# COMMAND ----------

# Create a transition request to staging and check it will trigger the test job to run
transition_request_body = {'name': model_name, 'version': 1, 'stage': 'Staging'}
mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(transition_request_body))

# COMMAND ----------

# Delete all webhooks
for webhook in webhooks_list:
  if webhook.id == job_webhook.id:
    RegistryWebhooksClient().delete_webhook(webhook.id)

# COMMAND ----------

# Verify webhook deletion
webhooks_list = RegistryWebhooksClient().list_webhooks()
for webhook in webhooks_list:
  if webhook.id == job_webhook.id:
    print(webhook)
assert job_webhook.id not in [w.id for w in webhooks_list]

# COMMAND ----------


