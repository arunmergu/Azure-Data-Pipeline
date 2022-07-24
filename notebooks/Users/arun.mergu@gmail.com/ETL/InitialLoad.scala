// Databricks notebook source
// MAGIC %md
// MAGIC Prerequisites:
// MAGIC 
// MAGIC Load the input datasets into dbfs and copy the same location to path in cmd3 below.
// MAGIC 
// MAGIC File -> Upload Data -> Select required files -> Upload to DBFS.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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

// Read input csv files from dbfs and enforce the schema objects created
val ratings = spark.read.format("csv").option("header",true).schema(ratings_schema).load("dbfs:/FileStore/shared_uploads/arun.mergu@gmail.com/ratings.csv")
ratings.show()

val movies = spark.read.format("csv").option("header",true).schema(movies_schema).load("dbfs:/FileStore/shared_uploads/arun.mergu@gmail.com/movies.csv")
movies.show()

val tags = spark.read.format("csv").option("header",true).schema(tags_schema).load("dbfs:/FileStore/shared_uploads/arun.mergu@gmail.com/tags.csv")
tags.show()

// COMMAND ----------

//write the dataframes to delta tables with appropriate partitioning
ratings.write
  .format("delta")
  .mode("overwrite")
  .option("path", "/mnt/delta/ratings")
  .partitionBy("userId") //Cardinality of userid is less than movieId
  .saveAsTable("ratings") // External table

// COMMAND ----------

movies.write
  .format("delta")
  .mode("overwrite")
  .option("path", "/mnt/delta/movies")
  .saveAsTable("movies"); // External table

tags.write
  .format("delta")
  .mode("overwrite")
  .option("path", "/mnt/delta/tags")
  .partitionBy("userId")
  .saveAsTable("tags"); // External table

// COMMAND ----------

// MAGIC %md
// MAGIC Qualilty checks for Initial loads

// COMMAND ----------

//Check: Check if tables have been created
assert(spark.catalog.tableExists("ratings"), "ratings Table doesnot exists")
assert(spark.catalog.tableExists("movies"), "movies Table doesnot exists")
assert(spark.catalog.tableExists("tags"), "movies Table doesnot exists")

//Check: Check if No. of columns match as expected
assert(ratings.schema.fieldNames.size==4, "ratings :Number of Columns mismatch")
assert(movies.schema.fieldNames.size==3, "movies: Number of Columns mismatch")
assert(tags.schema.fieldNames.size==4, "tags: Number of Columns mismatch")

//Check: Check if column names match as expected
assert(ratings.schema.fieldNames.contains("userId") && ratings.schema.fieldNames.contains("movieId") && ratings.schema.fieldNames.contains("rating")  && ratings.schema.fieldNames.contains("timestamp") , "Missing required columns")
assert(movies.schema.fieldNames.contains("movieId") && movies.schema.fieldNames.contains("title")  && movies.schema.fieldNames.contains("genres") , "movies: Missing required columns")
assert(tags.schema.fieldNames.contains("userId") && tags.schema.fieldNames.contains("movieId") && tags.schema.fieldNames.contains("tag")  && tags.schema.fieldNames.contains("timestamp") , "tags: Missing required columns")


//Check: Check if ratings column values are between 0 and 5 with increments of 0.5
val ratings_list = List(4.0, 5.0, 3.0, 2.0, 1.0, 4.5, 3.5, 2.5, 0.5, 1.5) 
assert(ratings.filter(!($"rating".isin(ratings_list: _*))).count() == 0, "Invalid ratings")



// COMMAND ----------

//Check: Check if column datatypes match 
assert(ratings.schema("userId").dataType.typeName == "integer" && 
ratings.schema("movieId").dataType.typeName == "integer" && 
ratings.schema("rating").dataType.typeName == "float" && 
ratings.schema("timestamp").dataType.typeName == "string" ,"Datatype mismatch")

assert(movies.schema("movieId").dataType.typeName == "integer" && 
movies.schema("title").dataType.typeName == "string" && 
movies.schema("genres").dataType.typeName == "string" ,"movies: Datatype mismatch")

assert(tags.schema("userId").dataType.typeName == "integer" && 
tags.schema("movieId").dataType.typeName == "integer" && 
tags.schema("tag").dataType.typeName == "string" && 
tags.schema("timestamp").dataType.typeName == "string" ,"tags: Datatype mismatch")

//Check: Check if there are no duplicates at userId and MovieId level before merging with delta table
assert(ratings.groupBy("userId", "movieId").count().filter(($"count") > 1 ).count() == 0, "Duplicate values at user and movie level")

//Check: Check if userId or MovieId columns have null values level before merging with delta table
assert(ratings.filter(($"userId" === "") or $"userId".isNull).count() ==  0, "UserId is having null values in Ratings ")
assert(ratings.filter(($"movieId" === "") or $"movieId".isNull).count() ==  0, "movieId is having null values in Ratings ")

// COMMAND ----------

