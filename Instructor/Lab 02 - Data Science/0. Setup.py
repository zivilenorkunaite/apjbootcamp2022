# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

red = pd.read_csv('/dbfs/FileStore/ajmal_aziz/bootcamp_data/winequality_red.csv')
white = pd.read_csv('/dbfs/FileStore/ajmal_aziz/bootcamp_data/winequality_white.csv', sep=";")

red['type'] = 'valencia orange'
white['type'] = 'navel orange'

# COMMAND ----------

table = pd.concat([red, white], axis=0, ignore_index=True)

# COMMAND ----------

table = table.rename(columns={"alcohol": "vitamin C", "volatile acidity": "enzymes", "free sulfur dioxide": "octyl acetate"})

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists anz_bootcamp3_ml_db;
# MAGIC use anz_bootcamp3_ml_db

# COMMAND ----------

table['type'].unique()

# COMMAND ----------

table = table.rename(columns={'fixed acidity': "fixed_acidity", 'citric acid': 'citric_acid', 'residual sugar': "residual_sugar", "octyl acetate": "octyl_acetate", "vitamin C": "vitamin_c", 'total sulfur dioxide': "total_sulfur_dioxide"})

# COMMAND ----------

table['target'] = ''
table.loc[table['quality'] >=6.5, 'target'] = 'Good'
table.loc[table['quality'] <6.5, 'target'] = 'Bad'

# COMMAND ----------

table = table.drop(columns=['quality'], axis=1)

# COMMAND ----------

table.rename(columns={'target': 'quality'}, inplace=True)

# COMMAND ----------

table_spark = spark.createDataFrame(table) 

# COMMAND ----------

len(table_spark.collect())

# COMMAND ----------

set([x.type for x in table_spark.collect()])

# COMMAND ----------

table_spark.write.format("delta").mode('overwrite').saveAsTable('phytochemicals_quality')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table oranges_phytochemicals_quality

# COMMAND ----------


