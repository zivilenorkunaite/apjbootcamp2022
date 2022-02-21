# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Machine Learning Introduction on Databricks
# MAGIC 
# MAGIC We start by understanding the business problem before translating to a technical problem. 
# MAGIC 
# MAGIC ## The Business Problem
# MAGIC APJuice is really interested in having the best quality ingredients for their juices. Their most popular juice flavours are Oranges. You might be thinking, "Simple! How different could oranges really be?" Well, actually, there's over 20 varieties of oranges and even more flavour profiles. The key indicators for flavour profiles of an orange are: Level of **acidity**, amount of **enzymes**, **citric acid** concentration, **sugar content**, **chlorides**, the aroma (**Octyl Acetate**), and amount of **sulfur dioxide**.
# MAGIC 
# MAGIC Clearly the flavour profile of an orange is quite complex. Additionally, easy of these variables that determine the taste have **differing marginal cost**. For example, increasing the amount of Octyl Acetate is **more expensive** than the amount of sugar. 
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align:center">
# MAGIC   <img src="files/ajmal_aziz/bootcamp_data/orange_classification.png" width="1000px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC APJuice are quite scientific and follow the Popperian view of science. Additionally, they are willing to accept that if a model can be derived that can model this relationship then the hypothesis has been proven true.
# MAGIC 
# MAGIC > **Hypothesis statement:** do the chemical properties influence the taste of an orange? If so, what is the best combination of chemical properties (financially) such that the quality is high but the cost is low?
# MAGIC 
# MAGIC As a starting point, APJuice collected some data from customers and what they thought of the quality of some oranges. We will test the hypothesis by training a machine learning model to predict quality scores from respondents. Let's start with some exploratory data analysis.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exploratory Data Analysis
# MAGIC 
# MAGIC Before diving into modelling, we want to analyse our collected data - this will inform our feature engineering and modelling processes. Some examples of questions we are looking to address:
# MAGIC 
# MAGIC 1. Are there any missing values: if so we'll need to impute them.
# MAGIC - Are there any highly correlated features? We can consolidate our predictors if so.
# MAGIC - Low/0 variance features: constant values won't be great predictors
# MAGIC - Will we need to scale our values?
# MAGIC - Can we create new features through feature crossing to learn non-linear relationships?

# COMMAND ----------

# MAGIC %run "./Utils/Setup-Datasets"

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Let's import the collected data from our Delta lake
db_name = "anz_bootcamp3_ml_db"
spark.sql(f"USE {db_name}")
collected_data = spark.read.table("phytochemicals_quality")
display(collected_data)

# COMMAND ----------

# DBTITLE 1,Let's use Databricks' built in data profiler to examine our dataset
display(collected_data)

# COMMAND ----------

# DBTITLE 1,Looks like we have a slightly positive relationship between vitamin C and oranges
display(collected_data)

# COMMAND ----------

display(collected_data)

# COMMAND ----------

display(collected_data)

# COMMAND ----------

# DBTITLE 1,We can also use the plotly within our notebooks 
import plotly.express as ps
import plotly.express as px
import plotly.io as pio

# Plotting preferences
pio.templates.default = "plotly_white"

# We can easily convert our dataframe to pandas
collected_data_pd = collected_data.toPandas()

fig = px.scatter(collected_data_pd,
                 x="vitamin_c",
                 y="enzymes",
                 color="quality")

fig.update_layout(font_family="Arial",
                  title="Enzymes as a function of Vitaminc C",
                  yaxis_title="Enzymes",
                  xaxis_title="Vitamin C",
                  legend_title_text="Rated Quality",
                  font=dict(size=20))

fig.show()

# COMMAND ----------


