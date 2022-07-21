package rick.part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder().
    appName("Common Types")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")


  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  //Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val prefferedFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)


  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), prefferedFilter.as("good_movie"))
  moviesWithGoodnessFlagsDF.where("good_movie")

  //negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  //Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating")/10 +col("IMDB_Rating"))/2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) //corr is an ACTION | Not an Transformation (Lazy)

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //Captalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  //Replace
  val peoplesDF = vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )



  //Exercise
  //Filter Cars DF by a list of car names obtained by an API call

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val complexRegEx = getCarNames.map(_.toLowerCase()).mkString("|")

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegEx, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
    .show()

  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)





}
