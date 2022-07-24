// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// COMMAND ----------

//Check: Check if table exists 
assert(spark.catalog.tableExists("ratings"), "ratings Table doesnot exists")
assert(spark.catalog.tableExists("movies"), "movies Table doesnot exists")
assert(spark.catalog.tableExists("tags"), "movies Table doesnot exists")


// COMMAND ----------

// Load the latest data from staging tables
val ratings_upd= spark.sql("SELECT * FROM ratings")
val movies_upd= spark.sql("SELECT * FROM movies")
val tags_upd= spark.sql("SELECT * FROM tags")

// COMMAND ----------

rat

// COMMAND ----------

//TRANSFORMATION

// COMMAND ----------

//Split pipe seperated genres into multiple rows and save the output to dbfs
def split_movie_genres(df: DataFrame): DataFrame = {
  val result_df = df.withColumn("genres", explode(split($"genres", "[|]")))
  result_df.coalesce(1).write.mode("overwrite").option("header","true").csv("/mnt/output/movie_geners_exploded/")

  return result_df
}

val movies_genres_expld = split_movie_genres(movies_upd)
movies_genres_expld.show(5)

// COMMAND ----------

//Logic to retrive top N movies by average rating ordered by avg rating of movies which have atleast 5 ratings given
def top_n_movies_by_avg_rating(df: DataFrame, n: Integer): DataFrame = {
  var result_df = df.groupBy("movieId").agg(mean("rating") as "avg_rating", count("userId") as "cnt_ratings").sort( $"avg_rating".desc).filter($"cnt_ratings" >= 5).limit(n)
  result_df.coalesce(1).write.mode("overwrite").option("header","true").csv("/mnt/output/top_"+n+"_movies")
  return result_df
}

val top_10_movies = top_n_movies_by_avg_rating(ratings_upd, 10)
top_10_movies.show()

// COMMAND ----------

