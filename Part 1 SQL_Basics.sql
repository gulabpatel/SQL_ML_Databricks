-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1.4 SQL in Notebooks
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
-- MAGIC * Create a database and table
-- MAGIC * Compute aggregate statistics against a dataset 
-- MAGIC * Create visualizations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Mounting Data
-- MAGIC 
-- MAGIC We are going to run a Classroom Setup script to mount the data we will be using throughout the class.
-- MAGIC 
-- MAGIC A [mount](https://docs.databricks.com/spark/latest/data-sources/aws/amazon-s3.html#mount-an-s3-bucket) is a pointer to a remote storage location (typically AWS or Azure), so we can access that data from within Databricks.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create a Database and Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SF Fire Department Calls for Service
-- MAGIC 
-- MAGIC Let's take a look at the [SF Fire Department Calls for Service](https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3/data) dataset. This dataset includes all fire units responses to calls.

-- COMMAND ----------

-- MAGIC %fs head /mnt/davis/fire-calls/fire-calls-truncated-comma.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For this class, we want to use a dedicated database to store our tables. Let's call it `Databricks`.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Databricks

-- COMMAND ----------

USE Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Table
-- MAGIC 
-- MAGIC Let's create a table using SQL called `FireCalls` so we can query it using SQL.

-- COMMAND ----------

DROP TABLE IF EXISTS fireCalls;

CREATE TABLE IF NOT EXISTS fireCalls
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-calls/fire-calls-truncated-comma.csv",
  inferSchema "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Running Spark SQL Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at a sample of the data.

-- COMMAND ----------

SELECT * FROM fireCalls LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which neighborhoods have the most fire calls?

-- COMMAND ----------

SELECT `Neighborhooods - Analysis Boundaries` as neighborhood, 
  COUNT(`Neighborhooods - Analysis Boundaries`) as count 
FROM FireCalls 
GROUP BY `Neighborhooods - Analysis Boundaries`
ORDER BY count DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Visualizing your Data
-- MAGIC 
-- MAGIC We can use the built-in Databricks visualization to see which neighborhoods have the most fire calls.

-- COMMAND ----------

SELECT `Neighborhooods - Analysis Boundaries` as neighborhood, 
  COUNT(`Neighborhooods - Analysis Boundaries`) as count 
FROM fireCalls 
GROUP BY `Neighborhooods - Analysis Boundaries`
ORDER BY count DESC

-- COMMAND ----------

SELECT `Neighborhooods - Analysis Boundaries` as neighborhood, 
  COUNT(`Neighborhooods - Analysis Boundaries`) as count 
FROM fireCalls 
GROUP BY `Neighborhooods - Analysis Boundaries`
ORDER BY count DESC

