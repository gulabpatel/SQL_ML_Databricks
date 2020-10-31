-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3.3 Accessing Data
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this notebook you:<br>
-- MAGIC 
-- MAGIC * Read data from a BLOB store
-- MAGIC * Read data in serial from JDBC
-- MAGIC * Read data in parallel from JDBC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DBFS Mounts and S3
-- MAGIC 
-- MAGIC Amazon Simple Storage Service (S3) is the backbone of Databricks workflows.  S3 offers data storage that easily scales to the demands of most data applications and, by colocating data with Spark clusters, Databricks quickly reads from and writes to S3 in a distributed manner.
-- MAGIC 
-- MAGIC The Databricks File System, or DBFS, is a layer over S3 that allows you to [mount S3 buckets](https://docs.databricks.com/spark/latest/data-sources/aws/amazon-s3.html#mount-aws-s3), making them available to other users in your workspace and persisting the data after a cluster is shut down.
-- MAGIC 
-- MAGIC In Azure Databricks, DBFS is backed by the Azure Blob Store. More documentation can be found [here](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html#mount-azure-blob-storage).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the first lesson, you uploaded data using the Databricks user interface.  This can be done by clicking Data on the left-hand side of the screen.  Here we'll be mounting data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Define your AWS credentials.  Below are defined read-only keys, the name of an AWS bucket, and the mount name we will be referring to in DBFS.
-- MAGIC 
-- MAGIC For getting AWS keys, take a look at <a href="https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html" target="_blank"> take a look at the AWS documentation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ACCESS_KEY = "AKIAJBRYNXGHORDHZB4A"
-- MAGIC # Encode the Secret Key to remove any "/" characters
-- MAGIC SECRET_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF".replace("/", "%2F")
-- MAGIC AWS_BUCKET_NAME = "davis-dsv1071/data/"
-- MAGIC MOUNT_NAME = "/mnt/davis-tmp"

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC Now mount the bucket [using the template provided in the docs.](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#mounting-an-s3-bucket)
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The code below includes error handling logic to handle the case where the mount is already mounted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC   dbutils.fs.mount("s3a://{}:{}@{}".format(ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME), MOUNT_NAME)
-- MAGIC except:
-- MAGIC   print("""{} already mounted. Unmount using `dbutils.fs.unmount("{}")` to unmount first""".format(MOUNT_NAME, MOUNT_NAME))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Next, explore the mount using `%fs ls` and the name of the mount.

-- COMMAND ----------

-- MAGIC %fs ls /mnt/davis-tmp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC In practice, always secure your AWS credentials.  Do this by either maintaining a single notebook with restricted permissions that holds AWS keys, or delete the cells or notebooks that expose the keys. After a cell used to mount a bucket is run, you can access the data in this mount point in any notebook or any cluster in Databricks, and share the mount between colleagues.

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You now have access to an unlimited store of data.  This is a read-only S3 bucket.  If you create and mount your own, you can write to it as well.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now unmount the data.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.unmount("/mnt/davis-tmp")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Serial JDBC Reads
-- MAGIC 
-- MAGIC Java Database Connectivity (JDBC) is an application programming interface (API) that defines database connections in Java environments.  Spark is written in Scala, which runs on the Java Virtual Machine (JVM).  This makes JDBC the preferred method for connecting to data whenever possible. Hadoop, Hive, and MySQL all run on Java and easily interface with Spark clusters.
-- MAGIC 
-- MAGIC Databases are advanced technologies that benefit from decades of research and development. To leverage the inherent efficiencies of database engines, Spark uses an optimization called predicate push down.  **Predicate push down uses the database itself to handle certain parts of a query (the predicates).**  In mathematics and functional programming, a predicate is anything that returns a Boolean.  In SQL terms, this often refers to the `WHERE` clause.  Since the database is filtering data before it arrives on the Spark cluster, there's less data transfer across the network and fewer records for Spark to process.  Spark's Catalyst Optimizer includes predicate push down communicated through the JDBC API, making JDBC an ideal data source for Spark workloads.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to confirm you are using the right driver.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC Class.forName("org.postgresql.Driver")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First we need to create the JDBC String.

-- COMMAND ----------

DROP TABLE IF EXISTS twitterJDBC;

CREATE TABLE IF NOT EXISTS twitterJDBC
USING org.apache.spark.sql.jdbc
OPTIONS (
  driver "org.postgresql.Driver",
  url "jdbc:postgresql://server1.databricks.training:5432/training",
  user "readonly",
  password "readonly",
  dbtable "training.Account"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We now have a twitter table.

-- COMMAND ----------

SELECT * FROM twitterJDBC LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Add a subquery to `dbtable`, which pushes the predicate to JDBC to process before transferring the data to your Spark cluster.

-- COMMAND ----------

DROP TABLE IF EXISTS twitterPhilippinesJDBC;

CREATE TABLE twitterPhilippinesJDBC
USING org.apache.spark.sql.jdbc
OPTIONS (
  driver "org.postgresql.Driver",
  url "jdbc:postgresql://server1.databricks.training:5432/training",
  user "readonly",
  password "readonly",
  dbtable "(SELECT * FROM training.Account WHERE location = 'Philippines') as subq"
)

-- COMMAND ----------

SELECT * FROM twitterPhilippinesJDBC LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Parallel JDBC Reads

-- COMMAND ----------

SELECT min(userID) as minID, max(userID) as maxID from twitterJDBC

-- COMMAND ----------

DROP TABLE IF EXISTS twitterParallelJDBC;

CREATE TABLE IF NOT EXISTS twitterParallelJDBC
USING org.apache.spark.sql.jdbc
OPTIONS (
  driver "org.postgresql.Driver",
  url "jdbc:postgresql://server1.databricks.training:5432/training",
  user "readonly",
  password "readonly",
  dbtable "training.Account",
  partitionColumn '"userID"',
  lowerBound 2591,
  upperBound 951253910555168768,
  numPartitions 25
)

-- COMMAND ----------

SELECT * from twitterParallelJDBC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %timeit sql("SELECT * from twitterJDBC").describe()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %timeit sql("SELECT * from twitterParallelJDBC").describe()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For additional options [see the Spark docs.](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
