// Databricks notebook source
// MAGIC %md
// MAGIC Incremental loads - staging
// MAGIC 
// MAGIC This Notebook is used for incremental loads of Movie datasets and merging with the delta tables.

// COMMAND ----------

//Check: Check if table exists before Incremental laods
assert(spark.catalog.tableExists("ratings"), "ratings Table doesnot exists")
assert(spark.catalog.tableExists("movies"), "movies Table doesnot exists")
assert(spark.catalog.tableExists("tags"), "movies Table doesnot exists")


// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// Prepare schema objects for the input datasets with proper datatypes
val ratings_schema = StructType(
  List(
    StructField("userId", IntegerType, false),
    StructField("movieId", IntegerType, false),
    StructField("rating", FloatType, true),
    StructField("timestamp", StringType, true)
  )
)

val movies_schema = StructType(
  List(
    StructField("movieId", IntegerType, false),
    StructField("title", StringType, true),
    StructField("genres", StringType, true),
  )
)

val tags_schema = StructType(
  List(
    StructField("userId", IntegerType, false),
    StructField("movieId", IntegerType, false),
    StructField("tag", StringType, true),
    StructField("timestamp", StringType, true)
  )
)

// COMMAND ----------

// Read incremental input csv files from dbfs and enforce the schema objects created
val ratings_inc = spark.read.format("csv").option("header",true).schema(ratings_schema).load("dbfs:/FileStore/shared_uploads/arun.mergu@gmail.com/ratings.csv")

val movies_inc = spark.read.format("csv").option("header",true).schema(movies_schema).load("dbfs:/FileStore/shared_uploads/arun.mergu@gmail.com/movies.csv")

val tags_inc = spark.read.format("csv").option("header",true).schema(tags_schema).load("dbfs:/FileStore/shared_uploads/arun.mergu@gmail.com/tags.csv")

// COMMAND ----------


//Check: Check if No. of columns match before merging with delta table
assert(ratings_inc.schema.fieldNames.size==4, "ratings_inc :Number of Columns mismatch")
assert(movies_inc.schema.fieldNames.size==3, "movies_inc: Number of Columns mismatch")
assert(tags_inc.schema.fieldNames.size==4, "tags_inc: Number of Columns mismatch")

//Check: Check if column names match before merging with delta table
assert(ratings_inc.schema.fieldNames.contains("userId") && ratings_inc.schema.fieldNames.contains("movieId") && ratings_inc.schema.fieldNames.contains("rating")  && ratings_inc.schema.fieldNames.contains("timestamp") , "Missing required columns")
assert(movies_inc.schema.fieldNames.contains("movieId") && movies_inc.schema.fieldNames.contains("title")  && movies_inc.schema.fieldNames.contains("genres") , "movies_inc: Missing required columns")
assert(tags_inc.schema.fieldNames.contains("userId") && tags_inc.schema.fieldNames.contains("movieId") && tags_inc.schema.fieldNames.contains("tag")  && tags_inc.schema.fieldNames.contains("timestamp") , "tags_inc: Missing required columns")

//Check: Check if ratings column values are between 0 and 5 with increments of 0.5
val ratings_list = List(4.0, 5.0, 3.0, 2.0, 1.0, 4.5, 3.5, 2.5, 0.5, 1.5) 
assert(ratings_inc.filter(!($"rating".isin(ratings_list: _*))).count() == 0, "Invalid ratings")

// COMMAND ----------

//Check: Check if column datatypes match before merging with delta table
assert(ratings_inc.schema("userId").dataType.typeName == "integer" && 
ratings_inc.schema("movieId").dataType.typeName == "integer" && 
ratings_inc.schema("rating").dataType.typeName == "float" && 
ratings_inc.schema("timestamp").dataType.typeName == "string" ,"Datatype mismatch")

assert(movies_inc.schema("movieId").dataType.typeName == "integer" && 
movies_inc.schema("title").dataType.typeName == "string" && 
movies_inc.schema("genres").dataType.typeName == "string" ,"movies_inc: Datatype mismatch")

assert(tags_inc.schema("userId").dataType.typeName == "integer" && 
tags_inc.schema("movieId").dataType.typeName == "integer" && 
tags_inc.schema("tag").dataType.typeName == "string" && 
tags_inc.schema("timestamp").dataType.typeName == "string" ,"tags_inc: Datatype mismatch")

//Check: Check if there are no duplicates at userId and MovieId level before merging with delta table
assert(ratings_inc.groupBy("userId", "movieId").count().filter(($"count") > 1 ).count() == 0, "Duplicate values at user and movie level")

//Check: Check if userId or MovieId columns have null values level before merging with delta table
assert(ratings_inc.filter(($"userId" === "") or $"userId".isNull).count() ==  0, "UserId is having null values in ratings_inc ")
assert(ratings_inc.filter(($"movieId" === "") or $"movieId".isNull).count() ==  0, "movieId is having null values in ratings_inc ")

// COMMAND ----------

// Create temp views on top of inc dataframes
ratings_inc.createOrReplaceTempView("ratings_inc")
movies_inc.createOrReplaceTempView("movies_inc")
tags_inc.createOrReplaceTempView("tags_inc")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Merge incremental temp views with final delta tables

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO ratings a
// MAGIC USING ratings_inc b
// MAGIC ON a.userId= b.userId and a.movieId = b.movieId
// MAGIC WHEN MATCHED 
// MAGIC   THEN UPDATE SET *
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT *

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO movies a
// MAGIC USING movies_inc b
// MAGIC ON a.movieId = b.movieId
// MAGIC WHEN MATCHED 
// MAGIC   THEN UPDATE SET *
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT *

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO tags a
// MAGIC USING tags_inc b
// MAGIC ON a.userId= b.userId and a.movieId = b.movieId and a.tag = b.tag
// MAGIC WHEN MATCHED 
// MAGIC   THEN UPDATE SET *
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT *

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from ratings union
// MAGIC select count(*) from movies union
// MAGIC select count(*) from tags;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC ## Compacting Small Files and Indexing
// MAGIC 
// MAGIC Small files can occur for a variety of reasons; in our case, we performed a number of operations where only one or several records were inserted.
// MAGIC 
// MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
// MAGIC 
// MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
// MAGIC 
// MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE ratings
// MAGIC ZORDER BY movieId

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE movies
// MAGIC ZORDER BY movieId

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE tags
// MAGIC ZORDER BY movieId

// COMMAND ----------

