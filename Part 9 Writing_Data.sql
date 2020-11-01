-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3.6 Writing Data
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this notebook you:<br>
-- MAGIC * Employ the writing design pattern
-- MAGIC * Control partitions for database writes using REPARTITION and COALESCE hints

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Writing From Spark<br>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC Writing to a database in Spark differs from other tools largely due to its distributed nature. There are a number of variables that can be tweaked to optimize performance, largely relating to how data is organized on the cluster. Partitions are the first step in understanding performant database connections.
-- MAGIC 
-- MAGIC **A partition is a portion of your total data set,** which is divided into many of these portions so Spark can distribute your work across a cluster. 
-- MAGIC 
-- MAGIC The other concept needed to understand Spark's computation is a slot (also known as a core). **A slot/core is a resource available for the execution of computation in parallel.** In brief, a partition refers to the distribution of data while a slot refers to the distribution of computation.
-- MAGIC 
-- MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/partitions-and-cores.png" style="height: 400px; margin: 20px"/></div>
-- MAGIC 
-- MAGIC As a general rule of thumb, the number of partitions should be a multiple of the number of cores. For instance, with 5 partitions and 8 slots, 3 of the slots will be underutilized. With 9 partitions and 8 slots, a job will take twice as long as it waits for the extra partition to finish.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Import the dataset.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW fireCallsCSV
USING CSV 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.csv",
    header "true",
    sep ":"
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Switch to the Python API to run write commands.  You can do this `%python` and then run `sql(" <My Normal SQL Query> ")`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = sql("SELECT * FROM fireCallsCSV")
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Write using the `.write(" < path> ")` method on the DataFrame.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("OVERWRITE").csv(username + "/fire-calls.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at the file we just wrote.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(username + "/fire-calls.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice that it wrote a number of different parts, one for each partition.  Now let's get a sense for how to check how many partitions our data has, which once again is in Python.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.rdd.getNumPartitions()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Controlling Concurrency<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the context of JDBC database writes, **the number of partitions determine the number of connections used to push data through the JDBC API.** There are two ways to control this parallelism:  
-- MAGIC 
-- MAGIC | Hint | Transformation Type | Use | Evenly distributes data across partitions? |
-- MAGIC | :----------------|:----------------|:----------------|:----------------| 
-- MAGIC | `SELECT /*+ COALESCE(n) */`   | narrow (does not shuffle data) | reduce the number of partitions | no |
-- MAGIC | `SELECT /*+ REPARTITION(n) */`| wide (includes a shuffle operation) | increase the number of partitions | yes |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a new temporary view that coalesces all of our data to a single partition.  This can be done using a hint.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW fireCallsCSV1p
  AS
SELECT /*+ COALESCE(1) */ * 
FROM fireCallsCSV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now check the number of partitions.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("SELECT * FROM fireCallsCSV1p").rdd.getNumPartitions()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We now have a single partition.  What if we want more partitions?  Let's use a `REPARTITION` hint to evenly distribute our data across 8 partitions.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW fireCallsCSV8p
  AS
SELECT /*+ REPARTITION(12) */ * 
FROM fireCallsCSV

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("SELECT * FROM fireCallsCSV8p").rdd.getNumPartitions()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now you can save the results.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("SELECT * FROM fireCallsCSV8p").write.mode("OVERWRITE").csv(username + "/fire-calls-repartitioned.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click the arrow next to `Spark Jobs` under the following code cell in order to see a breakdown of the job you triggered. Click the next arrow to see a breakdown of the stages.
-- MAGIC 
-- MAGIC When you repartitioned the DataFrame to 8 partitions, 8 stages were needed, one to write each partition of the data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the results.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(username + "/fire-calls-repartitioned.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC - Alter the number of concurrent writes using `SELECT /*+ COALESCE(n) */` or `SELECT /*+ REPARTITION(n) */`
-- MAGIC - Note that `coalesce` means something different in ANSI SQL (that is, to return the first non-null value)
-- MAGIC - Files like CSV and Parquet might look like single files, but Spark saves them as a folder of different parts 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
