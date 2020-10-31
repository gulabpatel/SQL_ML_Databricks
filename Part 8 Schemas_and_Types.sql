-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3.5 Schemas and Types
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this notebook you:<br>
-- MAGIC * Motivate the use of schemas and types
-- MAGIC * Read from JSON without a schema
-- MAGIC * Read from JSON with a schema

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Why Schemas Matter

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Schemas are at the heart of data structures in Spark.
-- MAGIC **A schema describes the structure of your data by naming columns and declaring the type of data in that column.** 
-- MAGIC Rigorously enforcing schemas leads to significant performance optimizations and reliability of code.
-- MAGIC 
-- MAGIC Why is open source Spark so fast, and why is [Databricks Runtime even faster?](https://databricks.com/blog/2017/07/12/benchmarking-big-data-sql-platforms-in-the-cloud.html) While there are many reasons for these performance improvements, two key reasons are:<br><br>
-- MAGIC * First and foremost, Spark runs first in memory rather than reading and writing to disk. 
-- MAGIC * Second, using DataFrames allows Spark to optimize the execution of your queries because it knows what your data looks like.
-- MAGIC 
-- MAGIC Two pillars of computer science education are data structures, the organization and storage of data and algorithms, and the computational procedures on that data.  A rigorous understanding of computer science involves both of these domains. When you apply the most relevant data structures, the algorithms that carry out the computation become significantly more eloquent.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schemas with Semi-Structured JSON Data
-- MAGIC 
-- MAGIC **Tabular data**, such as that found in CSV files or relational databases, has a formal structure where each observation, or row, of the data has a value (even if it's a NULL value) for each feature, or column, in the data set.  
-- MAGIC 
-- MAGIC **Semi-structured data** does not need to conform to a formal data model. Instead, a given feature may appear zero, once, or many times for a given observation.  
-- MAGIC 
-- MAGIC Semi-structured data storage works well with hierarchical data and with schemas that may evolve over time.  One of the most common forms of semi-structured data is JSON data, which consists of attribute-value pairs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from JSON w/ InferSchema
-- MAGIC 
-- MAGIC Reading in JSON isn't that much different than reading in CSV files.
-- MAGIC 
-- MAGIC Let's start with taking a look at all the different options that go along with reading in JSON files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JSON Lines
-- MAGIC 
-- MAGIC Much like the CSV reader, the JSON reader also assumes:
-- MAGIC * That there is one JSON object per line and
-- MAGIC * That it's delineated by a new-line.
-- MAGIC 
-- MAGIC This format is referred to as **JSON Lines** or **newline-delimited JSON** 
-- MAGIC 
-- MAGIC More information about this format can be found at <a href="http://jsonlines.org/" target="_blank">http://jsonlines.org</a>.
-- MAGIC 
-- MAGIC ** *Note:* ** *Spark 2.2 was released on July 11th 2016. With that comes File IO improvements for CSV & JSON, but more importantly, **support for parsing multi-line JSON and CSV files**. You can read more about that (and other features in Spark 2.2) in the <a href="https://databricks.com/blog/2017/07/11/introducing-apache-spark-2-2.html" target="_blank">Databricks Blog</a>.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at the sample of our JSON data.

-- COMMAND ----------

-- MAGIC %fs ls /mnt/davis/fire-calls/fire-calls-truncated.json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Like we did with the CSV file, we can use **&percnt;fs head ...** to take a look at the first few lines of the file.

-- COMMAND ----------

-- MAGIC %fs head /mnt/davis/fire-calls/fire-calls-truncated.json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Read the JSON File
-- MAGIC 
-- MAGIC The command to read in JSON looks very similar to that of CSV.
-- MAGIC 
-- MAGIC In addition to reading the JSON file, we will also print the resulting schema.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW fireCallsJSON
USING JSON 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.json"
  )

-- COMMAND ----------

DESCRIBE fireCallsJSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at the table.

-- COMMAND ----------

SELECT * FROM fireCallsJSON LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Review: Reading from JSON w/ InferSchema
-- MAGIC 
-- MAGIC While there are similarities between reading in CSV & JSON there are some key differences:
-- MAGIC * We only need one job even when inferring the schema.
-- MAGIC * There is no header which is why there isn't a second job in this case - the column names are extracted from the JSON object's attributes.
-- MAGIC * Unlike CSV which reads in 100% of the data, the JSON reader only samples the data.  
-- MAGIC **Note:** In Spark 2.2 the behavior was changed to read in the entire JSON file.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from JSON w/ a User Defined Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### User-Defined Schemas
-- MAGIC 
-- MAGIC Spark infers schemas from the data, as detailed in the example above.  Challenges with inferred schemas include:  
-- MAGIC <br>
-- MAGIC * Schema inference means Spark scans all of your data, creating an extra job, which can affect performance
-- MAGIC * Consider providing alternative data types (for example, change a `Long` to a `Integer`)
-- MAGIC * Consider throwing out certain fields in the data, to read only the data of interest

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW fireCallsJSON ( 
  `Call Number` INT,
  `Unit ID` STRING,
  `Incident Number` INT,
  `Call Type` STRING,
  `Call Date` STRING,
  `Watch Date` STRING,
  `Received DtTm` STRING,
  `Entry DtTm` STRING,
  `Dispatch DtTm` STRING,
  `Response DtTm` STRING,
  `On Scene DtTm` STRING,
  `Transport DtTm` STRING,
  `Hospital DtTm` STRING,
  `Call Final Disposition` STRING,
  `Available DtTm` STRING,
  `Address` STRING,
  `City` STRING,
  `Zipcode of Incident` INT,
  `Battalion` STRING,
  `Station Area` STRING,
  `Box` STRING,
  `Original Priority` STRING,
  `Priority` STRING,
  `Final Priority` INT,
  `ALS Unit` BOOLEAN,
  `Call Type Group` STRING,
  `Number of Alarms` INT,
  `Unit Type` STRING,
  `Unit sequence in call dispatch` INT,
  `Fire Prevention District` STRING,
  `Supervisor District` STRING,
  `Neighborhooods - Analysis Boundaries` STRING,
  `Location` STRING,
  `RowID` STRING
)
USING JSON 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.json"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at how much faster that process was!
-- MAGIC Previous command to read JSON file took 15.79s and user defined schema took only 0.68s.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Primitive and Non-primitive Types
-- MAGIC 
-- MAGIC The Spark [`types` package](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types) provides the building blocks for constructing schemas.
-- MAGIC 
-- MAGIC A primitive type contains the data itself.  The most common primitive types include:
-- MAGIC 
-- MAGIC | Numeric | General | Time |
-- MAGIC |-----|-----|
-- MAGIC | `FloatType` | `StringType` | `TimestampType` | 
-- MAGIC | `IntegerType` | `BooleanType` | `DateType` | 
-- MAGIC | `DoubleType` | `NullType` | |
-- MAGIC | `LongType` | | |
-- MAGIC | `ShortType` |  | |
-- MAGIC 
-- MAGIC Non-primitive types are sometimes called reference variables or composite types.  Technically, non-primitive types contain references to memory locations and not the data itself.  Non-primitive types are the composite of a number of primitive types such as an Array of the primitive type `Integer`.
-- MAGIC 
-- MAGIC The two most common composite types are `ArrayType` and `MapType`. These types allow for a given field to contain an arbitrary number of elements in either an Array/List or Map/Dictionary form.
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the [Spark documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types) for a complete picture of types in Spark.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Review: Reading from JSON w/ User-Defined Schema
-- MAGIC * Just like CSV, providing the schema avoids the extra jobs.
-- MAGIC * The schema allows us to rename columns and specify alternate data types.
-- MAGIC * Can get arbitrarily complex in its structure.
