package rick.part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App{

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")


  // selecting the first non-null-value

  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating")*10)
  )

  //Checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // Nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  //Remove/Replace Nulls
  moviesDF.select(col("Title"), col("IMDB_Rating")).na.drop() //remove rows containing nulls

  //Replace
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten,Tomatoes_Rating"))

  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  //complex Operations

  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // coalesce
    "NVL(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl ", //same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", //Return null if the two values are equal
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2"
  )

}
