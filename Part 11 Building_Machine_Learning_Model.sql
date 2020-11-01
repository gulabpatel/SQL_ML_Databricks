-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Building a Machine Learning Model
-- MAGIC ## Module 4, Lesson 5
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:
-- MAGIC * Build a Machine Learning model using scikit-learn
-- MAGIC * Predict the response time to an incident given different features

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load Data
-- MAGIC 
-- MAGIC We are going to build a model on a subset of our data.

-- COMMAND ----------

USE DATABRICKS;

CREATE TABLE IF NOT EXISTS fireCallsClean
USING parquet
OPTIONS (
  path "/mnt/davis/fire-calls/fire-calls-clean.parquet"
)

-- COMMAND ----------

Describe fireCallsClean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Timestamp
-- MAGIC 
-- MAGIC Let's convert our `Response_DtTm` and `Received_DtTm` into timestamp types.

-- COMMAND ----------

CREATE OR REPLACE VIEW time AS (
  SELECT *, unix_timestamp(Response_DtTm, "MM/dd/yyyy hh:mm:ss a") as ResponseTime, 
            unix_timestamp(Received_DtTm, "MM/dd/yyyy hh:mm:ss a") as ReceivedTime
  FROM fireCallsClean
)

-- COMMAND ----------

DESCRIBE time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time Delay
-- MAGIC 
-- MAGIC Now that we have our `Response_DtTm` and `Received_DtTm` as timestamp types, we can compute the difference in minutes between the two.

-- COMMAND ----------

CREATE OR REPLACE VIEW timeDelay AS (
  SELECT *, (ResponseTime - ReceivedTime)/60 as timeDelay
  FROM time
)

-- COMMAND ----------

DESCRIBE timeDelay

-- COMMAND ----------

DESCRIBE EXTENDED timeDelay

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Uh oh! We have some records with a negative time delay, and some with very extreme values. We will filter out those records.

-- COMMAND ----------

SELECT timeDelay, Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Number_of_Alarms, Original_Priority, Unit_Type
FROM timeDelay 
WHERE timeDelay < 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great! Our data is prepped and ready to be used to build a model!

-- COMMAND ----------

SELECT timeDelay, Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Number_of_Alarms, Original_Priority, Unit_Type
FROM timeDelay 
WHERE timeDelay < 15 AND timeDelay > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Convert to Pandas DataFrame
-- MAGIC 
-- MAGIC We are going to convert our Spark DataFrame to a Pandas DataFrame to build a scikit-learn model. Although we could use SparkML to train models, a lot of data scientists start by building their models using Pandas and Scikit-Learn.
-- MAGIC 
-- MAGIC We will also enable [Apache Arrow](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html) for faster transfer of data from Spark DataFrames to Pandas DataFrames.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "true")
-- MAGIC 
-- MAGIC pdDF = sql("""SELECT timeDelay, Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Number_of_Alarms, Original_Priority, Unit_Type
-- MAGIC               FROM timeDelay 
-- MAGIC               WHERE timeDelay < 15 AND timeDelay > 0""").toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualize
-- MAGIC 
-- MAGIC Let's visualize the distribution of our time delay. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC 
-- MAGIC fig = pdDF.hist(column="timeDelay")
-- MAGIC display(fig.figure)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Train-Test Split
-- MAGIC 
-- MAGIC In this notebook we are going to use 80% of our data to train our model, and 20% to test our model. We set a [random_state](http://scikit-learn.org/stable/modules/generated/sklearn.model_selection.train_test_split.html) for reproducibility.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.model_selection import train_test_split
-- MAGIC 
-- MAGIC X = pdDF.drop("timeDelay", axis=1)
-- MAGIC y = pdDF["timeDelay"].values
-- MAGIC X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Baseline Model
-- MAGIC 
-- MAGIC Before we get started building our linear regression model, let's establish our baseline RMSE on our test dataset by always predicting the average value. Here, we are going to take the square root of the [MSE](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.mean_squared_error.html).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.metrics import mean_squared_error
-- MAGIC import numpy as np
-- MAGIC 
-- MAGIC avgDelay = np.full(y_test.shape, np.mean(y_train), dtype=float)
-- MAGIC 
-- MAGIC print("RMSE is {0}".format(np.sqrt(mean_squared_error(y_test, avgDelay))))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Build Linear Regression Model
-- MAGIC 
-- MAGIC Great! Now that we have established a baseline, let's use scikit-learn's [pipeline](http://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html) API to build a linear regression model.
-- MAGIC 
-- MAGIC Our pipeline will have two steps: 
-- MAGIC 0. [One Hot Encoder](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder): this converts our categorical features into numeric features by creating a dummy column for each value in that category. 
-- MAGIC     * For example, if we had a column called `Animal` with the values `Dog`, `Cat`, and `Bear`, the corresponding one hot encoding representation for Dog would be: `[1, 0, 0]`, Cat: `[0, 1, 0]`, and Bear: `[0, 0, 1]`
-- MAGIC 0. [Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html) model: find the line of best fit for our training data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.linear_model import LinearRegression
-- MAGIC from sklearn.preprocessing import OneHotEncoder
-- MAGIC from sklearn.pipeline import Pipeline
-- MAGIC 
-- MAGIC ohe = ("ohe", OneHotEncoder(handle_unknown="ignore"))
-- MAGIC lr = ("lr", LinearRegression(fit_intercept=True, normalize=True))
-- MAGIC 
-- MAGIC pipeline = Pipeline(steps = [ohe, lr]).fit(X_train, y_train)
-- MAGIC y_pred = pipeline.predict(X_test)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can see the corresponding one hot encoded feature names below.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(pipeline.steps[0][1].get_feature_names())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Evaluate on Test Data
-- MAGIC 
-- MAGIC Let's take a look at our RMSE.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.metrics import mean_squared_error
-- MAGIC 
-- MAGIC print("RMSE is {0}".format(np.sqrt(mean_squared_error(y_test, y_pred))))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Save Model
-- MAGIC 
-- MAGIC Not bad! We did a bit better than our baseline model. 
-- MAGIC 
-- MAGIC Let's save this model using MLflow. [MLflow](https://mlflow.org/) is an open-source project created by Databricks to help simplify the Machine Learning life cycle. 
-- MAGIC 
-- MAGIC While MLflow is out of the scope of this class, it has a nice function to generate Spark User-Defined Function (UDF) to apply this model in parallel to the rows in our dataset. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC !pip install mlflow

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC from mlflow.sklearn import save_model
-- MAGIC 
-- MAGIC model_path = "/dbfs/" + username + "/firecalls_pipeline"
-- MAGIC dbutils.fs.rm(username + "/firecalls_pipeline", recurse=True)
-- MAGIC save_model(pipeline, model_path)
