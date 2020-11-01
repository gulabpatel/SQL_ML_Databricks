-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3.7 Managed and Unmanaged Tables
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this notebook you:<br>
-- MAGIC * Write to managed and unmanaged tables
-- MAGIC * Explore the effect of dropping tables on the metadata and underlying data

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Managed and Unmanaged Tables<br>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC A **managed table** is a table that manages both the data itself as well as the metadata.  In this case, a `DROP TABLE` command removes both the metadata for the table as well as the data itself.  
-- MAGIC 
-- MAGIC **Unmanaged tables** manage the metadata from a table such as the schema and data location, but the data itself sits in a different location, often backed by a blob store like the Azure Blob or S3. Dropping an unmanaged table drops only the metadata associated with the table while the data itself remains in place.
-- MAGIC 
-- MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/managed-and-unmanaged-tables.png" style="height: 400px; margin: 20px"/></div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Start with a managed table.

-- COMMAND ----------

USE default;

DROP TABLE IF EXISTS tableManaged;

CREATE TABLE tableManaged (
  var1 INT,
  var2 INT
);

INSERT INTO tableManaged
  VALUES (1, 1), (2, 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use `DESCRIBE EXTENDED` to describe the contents of the table.  Scroll down to see the table `Type`.
-- MAGIC 
-- MAGIC Notice the location is also `dbfs:/user/hive/warehouse/< your database >/tablemanaged`.

-- COMMAND ----------

DESCRIBE EXTENDED tableManaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now use an external, or unmanaged, table

-- COMMAND ----------

DROP TABLE IF EXISTS tableUnmanaged;

CREATE EXTERNAL TABLE tableUnmanaged (
  var1 INT,
  var2 INT
)
STORED AS parquet
LOCATION '/tmp/unmanagedTable'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Describe the table and look for the `Type`

-- COMMAND ----------

DESCRIBE EXTENDED tableUnmanaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is an external, or managed table.  If we were to shut down our cluster, this data will persist.  Now insert values into the table.

-- COMMAND ----------

INSERT INTO tableUnmanaged
  VALUES (1, 1), (2, 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at the result.

-- COMMAND ----------

SELECT * FROM tableUnmanaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now view the underlying files in where the data was persisted.

-- COMMAND ----------

-- MAGIC %fs ls /tmp/unmanagedTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Dropping Managed and Unmanaged Tables<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Confirm that the underlying files exist for the managed table.

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/tablemanaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now drop the managed table.

-- COMMAND ----------

DROP TABLE tableManaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look--the files are gone!

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/tablemanaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now drop the unmanaged, or external, table.

-- COMMAND ----------

DROP TABLE tableUnmanaged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now take a look at the underlying files.

-- COMMAND ----------

-- MAGIC %fs ls /tmp/unmanagedTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC They're still there!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC - Use external/unmanaged tables when you want to persist your data once the cluster has shut down
-- MAGIC - Use managed tables when you only want ephemeral data

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

-- COMMAND ----------


