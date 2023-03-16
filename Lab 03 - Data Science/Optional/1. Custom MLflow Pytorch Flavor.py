# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Custom mlflow pytorch flavour
# MAGIC 
# MAGIC <div style="float:right"><img src="https://upload.wikimedia.org/wikipedia/commons/9/96/Pytorch_logo.png" width=250/></div>
# MAGIC <div style="float:right"><img src="https://databricks.com/wp-content/uploads/2021/06/MLflow-logo-pos-TM-1.png" width=250/></div>
# MAGIC 
# MAGIC ```mlflow``` is also able to handle abitrary machine learning models using mlflow's ```pyfunc``` model flavour.
# MAGIC 
# MAGIC Let's create a simple neural network model for our orange quality prediction and see how mlflow handles it.

# COMMAND ----------

# MAGIC %run "../Utils/Fetch_User_Metadata"

# COMMAND ----------

# DBTITLE 1,Note: Prepare the training data same as notebook ../4. Machine Learning
from sklearn.model_selection import train_test_split
from databricks.feature_store import FeatureLookup, FeatureStoreClient
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler
from sklearn.compose import ColumnTransformer

spark.sql(f"USE {DATABASE_NAME}")
data = spark.table("phytochemicals_quality")

train_portion = 0.7
valid_portion = 0.2
test_portion = 0.1

train_df, test_df = train_test_split(data.toPandas(), test_size=test_portion)
train_df, valid_df = train_test_split(train_df, test_size= 1 - train_portion/(train_portion+valid_portion))

fs = FeatureStoreClient()

feature_table = f"{DATABASE_NAME}.features_oj_prediction_experiment"

feature_lookup = FeatureLookup(
  table_name=feature_table,
  #
  #          Pull our calculated features from our feature store
  #             |
  #             |
  feature_names=["h_concentration", "acidity_ratio"],
  lookup_key = ["customer_id"]
)

training_set = fs.create_training_set(
  df=spark.createDataFrame(train_df),
  feature_lookups=[feature_lookup],
  label = 'quality',
  exclude_columns="customer_id"
)

validation_set = fs.create_training_set(
  df=spark.createDataFrame(valid_df),
  feature_lookups=[feature_lookup],
  label = 'quality',
  exclude_columns="customer_id"
)

training_data = training_set.load_df().toPandas()
validation_data = validation_set.load_df().toPandas()

# Gather and exogenous and endogenous variables from training and validation
X_training = training_data.drop(columns=['quality'], axis=1)
y_training = (training_data['quality'] == "Good").astype(int)

# MLflow 1.x required validation in this format
X_validation = validation_data.drop(columns=['quality'], axis=1)
y_validation = (validation_data['quality'] == "Good").astype(int)

# MLFLow API change in 2.x means we need the validation as a single data table
validation_dataset = validation_data
validation_dataset['quality'] = (validation_data['quality'] == "Good").astype(int)

# Gather the numerical and categorical features
numerical_features = X_training._get_numeric_data().columns.tolist()
categorical_features = list(set(X_training.columns) - set(numerical_features))

preprocessing_pipeline = []

one_hot_pipeline = Pipeline(steps=[
    ("imputer", SimpleImputer(missing_values=None, strategy="constant", fill_value="")),
    ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessing_pipeline.append(("process_categorical", one_hot_pipeline, categorical_features))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputer", SimpleImputer(strategy="mean")),
    ("scaler", StandardScaler())
])

preprocessing_pipeline.append(("process_numerical", numerical_pipeline, numerical_features))

preprocessor = ColumnTransformer(preprocessing_pipeline, remainder="passthrough", sparse_threshold=0)

# COMMAND ----------

nn_params = {'num_epochs': 2,
             'batch_size': 128,
             'learning_rate': 0.001}

# COMMAND ----------

# DBTITLE 1,We specify our dataset primitive which will turn into an iterator for training
import torch
import mlflow
import pandas as pd
from torch.utils.data import Dataset, DataLoader

class Data(Dataset):
    def __init__(self, X_data, y_data):
        self.X_data = torch.FloatTensor(X_data)
        self.y_data = torch.FloatTensor(y_data)
        
    def __getitem__(self, index):
        return self.X_data[index], self.y_data[index]

    def __len__ (self):
      return len(self.X_data)

# COMMAND ----------

# DBTITLE 1,As we normally would, we pre-process the raw training data
# Apply our preprocessing pipeline only
X_training_fit = preprocessor.fit_transform(X_training)
X_validation_fit = preprocessor.transform(X_validation)

# Define the Data primitive for training, validation, and testing
training_data = Data(X_training_fit, y_training)
validation_data = Data(X_validation_fit, y_validation)

# COMMAND ----------

# DBTITLE 1,Define our DataLoader iterables around our Datasets (training, validation, testing)
train_loader = DataLoader(dataset=training_data, batch_size=nn_params['batch_size'], shuffle=True)
valid_loader = DataLoader(dataset=validation_data, batch_size=1)

# COMMAND ----------

# DBTITLE 1,We define our very simple neural net architecture
from torch import nn

class BinaryClassifier(nn.Module):
    def __init__(self):
        super(BinaryClassifier, self).__init__()
        
        self.layer_1 = nn.Linear(15, 64) 
        self.layer_2 = nn.Linear(64, 64)
        #                               
        #                            Single output
        #                              |
        self.layer_out = nn.Linear(64, 1)
        
        self.sigmoid = nn.Sigmoid()
        self.relu = nn.ReLU()
        self.dropout = nn.Dropout(p=0.1)
        self.batchnorm1 = nn.BatchNorm1d(64)
        self.batchnorm2 = nn.BatchNorm1d(64)
        
    def forward(self, inputs):
        x = self.relu(self.layer_1(inputs))
        x = self.batchnorm1(x)
        x = self.relu(self.layer_2(x))
        x = self.batchnorm2(x)
        x = self.dropout(x)
        x = self.layer_out(x)
        
        return self.sigmoid(x)

# COMMAND ----------

# DBTITLE 1,We define our NN model, the loss function, and the optimiser
from torch import optim

model = BinaryClassifier()
criterion = torch.nn.BCEWithLogitsLoss()
optimiser = optim.Adam(model.parameters(), lr=nn_params['learning_rate'])

# COMMAND ----------

# DBTITLE 1,Train our model and manually log metrics using mlflow
from sklearn.metrics import roc_auc_score

def train_model(model, train_loader, nn_params, optimiser):
  model.train()
  for step in range(nn_params['num_epochs']):
    for X_batch, y_batch in train_loader:
      optimiser.zero_grad()
      y_pred = model(X_batch)

      loss = criterion(y_pred, y_batch.unsqueeze(1))
      acc = roc_auc_score(y_batch.squeeze().detach().numpy(),
                          y_pred.squeeze().detach().numpy())

      # 🔙 Backprop :) 
      loss.backward()
      optimiser.step()

      mlflow.log_metric("Epoch Loss", float(loss.item()), step=step)
      mlflow.log_metric("Model Training AUC", acc, step=step)
  return model

# COMMAND ----------

# DBTITLE 1,Now we define our mlflow pyfunc model wrapper
class TorchClassifer(mlflow.pyfunc.PythonModel):
  def __init__(self, model, params):
    self.model = model
    self.params = params
    self.model.eval()

  def predict(self, context, X):
    with torch.no_grad():
      return self.model(X).detach().numpy()

# COMMAND ----------

# DBTITLE 1,Create a method for evaluating our trained model
def evaluate_model(model, valid_loader):
  # Now we can evaluate our model:
  y_true, y_pred = [], []
  for X_valid, y_valid in valid_loader:
    y_pred.append(model_pyfunc.predict(None, X_valid)[0])
    y_true.append(y_valid.detach().numpy()[0])
  return roc_auc_score(y_true, y_pred)

# COMMAND ----------

# DBTITLE 1,Finally, we can run within a run context to store, log, and track model training and validation
from mlflow.models import infer_signature

with mlflow.start_run(run_name="Pytorch Model") as run:
  # Log the parameters of the model run
  mlflow.log_params(nn_params)
  run_id = run.info.run_id
  
  # Train the model
  trained_model = train_model(model, train_loader, nn_params, optimiser)
  model_pyfunc = TorchClassifer(trained_model, nn_params)
  
  # Evaluate the model
  valid_roc_auc = evaluate_model(model_pyfunc, valid_loader)
  mlflow.log_metric("Validation AUC", valid_roc_auc)
  
  # Log the model object
  X_batch, _ = next(iter(valid_loader))
  #   signature = infer_signature(X_batch, model_pyfunc.predict(None, X_batch))
  mlflow.pyfunc.log_model("pytorch_model", python_model=model_pyfunc)

# COMMAND ----------


