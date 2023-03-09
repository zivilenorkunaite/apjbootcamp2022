# Databricks notebook source
import json

from databricks_cli.sdk import ApiClient
from databricks_cli.pipelines.api import PipelinesApi

_notebook_ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

api_host = 'https://' + _notebook_ctx['tags']['browserHostName']
api_token = _notebook_ctx['extraContext']['api_token']


# Provide a host and token
db = DatabricksAPI(
    host=api_host,
    token=api_token
)


# COMMAND ----------

dlt_pipeline_name = "apjbootcamp-DLT-demo"
target_db = "apjdatabricksbootcamp"
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

dlt_pipeline_location = f"/Repos/{current_user_id}/apjbootcamp2022/Lab 0X - Data Engineering/02 - Transform"
notebook_paths = [{"notebook": {"path": dlt_pipeline_location}}]

cluster_config = [{"label": "default", "num_workers": 1}]

p = db.client.perform_query(
    "get", f"/pipelines?filter=name LIKE '{dlt_pipeline_name}'"
).get("statuses", [])

if len(p) > 1:
    print("Pipeline already exists. Choose a different name")
elif not p:
    print("Pipeline not found. Will create and run a new one")
    new_pipeline = db.delta_pipelines.create(
        id=None,
        name=dlt_pipeline_name,
        storage="/tmp/databricksbootcamp/dlt",
        configuration=dlt_configuration,
        clusters=cluster_config,
        libraries=notebook_paths,
        trigger=None,
        filters=None,
        target=target_db,
        continuous=False,
        development=True,
        allow_duplicate_names=False,
        headers=None,
    )
    db.delta_pipelines.run(pipeline_id=new_pipeline['pipeline_id'])
    #spark.sql(f"GRANT SELECT on {target_db} to users")
