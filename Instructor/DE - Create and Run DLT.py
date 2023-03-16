# Databricks notebook source
dlt_pipeline_name = "apjdatabricksbootcamp-DLT-demo"
workflow_name = "apjdatabricksbootcamp-workflow-demo"
target_db = "apjdatabricksbootcamp"
sql_query_name = "apjdatabricksbootcamp-SQL-demo"

# COMMAND ----------

import json
import requests

from databricks_cli.sdk import ApiClient
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.jobs.api import JobsApi

current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

_notebook_ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

api_host = 'https://' + _notebook_ctx['tags']['browserHostName']
api_token = _notebook_ctx['extraContext']['api_token']

# Provide a host and token
db = ApiClient(
    host=api_host,
    token=api_token
)

jobs = JobsApi(db)
pipelines = PipelinesApi(db)

# COMMAND ----------

def get_warehouse_ids():
  warehouse_uuid = ''
  warehouse_id = ''

  all_warehouses = requests.get(
    f'{api_host}/api/2.0/sql/warehouses',
    headers={'Authorization': f'Bearer {api_token}'},
  )

  if all_warehouses.status_code == 200:
    find_serverless_and_pro = [i for i in all_warehouses.json()['warehouses'] if (i['warehouse_type'] == 'PRO' ) and i['state'] == 'RUNNING' ]
    if len(find_serverless_and_pro) == 0:
      find_serverless_and_pro = [i for i in all_warehouses.json()['warehouses'] if (i['warehouse_type'] == 'PRO' )]
    if len(find_serverless_and_pro) == 0:
      print("No warehouses found")
    else:
      warehouse_id = find_serverless_and_pro[0]['id']
    
    # get data source id for this warehouse
    all_data_sources = requests.get(
      f'{api_host}/api/2.0/preview/sql/data_sources',
      headers={'Authorization': f'Bearer {api_token}'},
    )

    if all_data_sources.status_code == 200:
      find_uuid =  [i for i in all_data_sources.json() if (i['warehouse_id'] == warehouse_id ) ]
      warehouse_uuid = find_uuid[0]['id']
    else:
      print("Error getting data sources: %s" % (all_data_sources.json()))
  else:
    print("Error getting warehouses: %s" % (all_warehouses.json()))
  
  return warehouse_uuid, warehouse_id
  
def create_new_sql_query(warehouse_id):
  
  query_id = ''
  
  sql_text = """
  select count(1) sales_last_7_days from apjdatabricksbootcamp.silver_sales
  where ts >= date_add(now(), -7)
  and store_id = {{ param }}
  """

  if warehouse_id != '':
    new_query = requests.post(
        f'{api_host}/api/2.0/preview/sql/queries',
        headers={'Authorization': f'Bearer {api_token}'},
      json={
        "data_source_id": warehouse_id,
        "description": "Demo SQL query",
        "name": sql_query_name,
        "options": {
          "parameters": [
            {
              "name": "param",
              "title": "store_id",
              "type": "text",
              "value": "AKL01"
            }
          ]
      },
        "query": sql_text,
      }
    )

    if new_query.status_code == 200:
      print(new_query.json()['id'])
    else:
      print("Error creating query: %s" % (new_query.json()))
      
    query_id = new_query.json()['id']
    return query_id
  
def create_dlt_pipeline():
  pipeline_id = ''
  
  dlt_pipeline_location = f"/Repos/{current_user_id}/apjbootcamp2022/Instructor/DE - Lab - 02 - Transform (Full)"
  notebook_paths = [{"notebook": {"path": dlt_pipeline_location}}]
  dlt_configuration = []

  cluster_config = [{"label": "default", "num_workers": 1}]

  p = db.perform_query(
      "get", f"/pipelines?filter=name LIKE '{dlt_pipeline_name}'"
  ).get("statuses", [])

  if len(p) > 0:
      print("Pipeline with given name already exists. Reusing existing one")
      pipeline_id = p[0]['pipeline_id']
  else:
      new_pipeline = pipelines.client.create(
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
      pipeline_id = new_pipeline['pipeline_id']
  return pipeline_id

def create_new_job(dlt_pipeline_id, query_id, warehouse_id):
  job_id = ''
  if api_host.endswith(".azuredatabricks.net"):
    # Azure instance type
    instance_type = 'Standard_DS3_v2'
  else:
    # Use AWS instance type
    instance_type = 'i3.xlarge'
  
  new_job = jobs.client.create_job(
    name=workflow_name,
    max_concurrent_runs=1,
    tasks=[
        {
            "task_key": "ingest_dataset",
            "description": "Ingest dataset for DLT",
            "notebook_task": {
                "notebook_path": f"/Repos/{current_user_id}/apjbootcamp2022/Instructor/DE - Lab - 01 - Ingest Data (Full)"
            },
            "max_retries": 1,
            "retry_on_timeout": "false",
            "new_cluster": {
                "spark_version": "12.2.x-cpu-ml-scala2.12",
                "node_type_id": instance_type,
                "autoscale": {"min_workers": 1, "max_workers": 2},
            },
        },
        {
            "task_key": "transform_data",
            "description": "Transform data using DLT",
            "pipeline_task": {
                "pipeline_id": dlt_pipeline_id,
                "full_refresh": "false",
            },
            "depends_on": [{"task_key": "ingest_dataset"}],
        },
        {
            "task_key": "update_sql_query",
            "description": "Refresh SQL query",
             "sql_task": {
                    "query": {
                        "query_id": query_id
                    },
                    "warehouse_id": warehouse_id
                },
            "depends_on": [{"task_key": "transform_data"}],
        },
    ],
  )
  return new_job['job_id']

def run_that_job(job_id):
  job_run = requests.post(
        f'{api_host}/api/2.1/jobs/run-now',
        headers={'Authorization': f'Bearer {api_token}'},
      json={
        "job_id": job_id
      }
    )
  if job_run.status_code == 200:
    print(job_run.json())
  else:
    print("Error creating query: %s" % (job_run.json()))

# COMMAND ----------

warehouse_uuid, warehouse_id = get_warehouse_ids()
query_id = create_new_sql_query(warehouse_uuid)

dlt_pipeline_id = create_dlt_pipeline()


if warehouse_uuid != '' and dlt_pipeline_id != '' and query_id != '':
  new_job_id = create_new_job(dlt_pipeline_id, query_id, warehouse_id)
  run_that_job(new_job_id)
else:
  print(f"Not all elements were created. {warehouse_id=}, {query_id=}, {dlt_pipeline_id=}")
