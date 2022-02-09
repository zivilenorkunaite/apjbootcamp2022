# Databricks notebook source
import databricks.automl
import mlflow

# COMMAND ----------

kwargs = {
  'dataset': spark.table('phytochemicals'),
  'max_trials': 10e6,
  "target_col": target_col,
  
}

# COMMAND ----------

classifier = databricks.automl.classifier.Classifier(context_type=databricks.automl.ContextType.DATABRICKS)
classifier.fit(**kwargs)
