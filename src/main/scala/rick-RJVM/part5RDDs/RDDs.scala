package part5RDDs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source


object RDDs extends App{

  val spark = SparkSession.builder()
    .appName("Intro to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - Parallelize an exsting collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - Reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) = {
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  //2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - EASY - read from a DF

  val stocksDF = spark.read
    .option("header", "true").option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksRDD4 = stocksDF.rdd // LOSE THE TYPE INFORMATION
  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // FROM RDD TO DF (LOSE THE TYPE INFORMATION)
  val numbersDF = numbersRDD.toDF("numbers")

  // FROM RDD TO DS (KEEP TYPE INFORMATION)
  val numbersDS = spark.createDataset(numbersRDD)


  //Transformation
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") //Lazy
  val msCount = msftRDD.count() // Action

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() //Lazy

  // min or max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa: StockValue ,sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min()


  //reduce
  numbersRDD.reduce(_ + _)

  //grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // EXPENSIVE


  //Paritioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write.mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /*
  REPARTITION IS EXPENSIVE. Involve Shuffling and traffic
  Best Practice: partition early, then process that
    - size of a partition 10-100MB
   */

  // COALESCE
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling
  coalescedRDD.toDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks15")

  /**
    *EXERCISES
    * 1 - Read the movies.json as an RDD. Using the case class defined bellow
    * 2 - Show the distinct genres as an RDD.
    * 3 - Select all the movies in the Drama genre with IMDB Rating > 6
    * 4 - Show the average rating of movies by genre

    */


  case class Movie(title: String, genre:String, rating: Double)


  //1
  val moviesRDD2 = sc.textFile("src/main/resources/data/movies.json")
    .map(line => line.split(","))
    .map(tokens => Movie(tokens(0).split(":")(1).replaceAll("\"", ""),
      tokens(9).split(":")(1).replaceAll("\"", ""),
      tokens(13).split(":")(1).replace("null","0").toDouble)
  )

  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")


  val moviesRDD = moviesDF.select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  moviesRDD.toDF.show(false)
  //2
  val distinctMoviesRDD = moviesRDD.map(_.genre).distinct() //Lazy
  distinctMoviesRDD.toDF.show(false)

  //3

  val dramaMoviesRDD = moviesRDD.filter(x => (x.genre == "Drama" && x.rating >= 6))
  dramaMoviesRDD.toDF.show(false)



  // 4 - Show the average rating of movies by genre
  case class GenreAvgRating(genre: String, rating:Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map{
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show

  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show()


}



/*{"Title":"The Land Girls",
"US_Gross":146083,
"Worldwide_Gross":146083,
"US_DVD_Sales":null,
"Production_Budget":8000000,
"Release_Date":"12-Jun-98",
"MPAA_Rating":"R",
"Running_Time_min":null,
"Distributor":"Gramercy",
"Source":null,
"Major_Genre":null,
"Creative_Type":null,
"Director":null,
"Rotten_Tomatoes_Rating":null,
"IMDB_Rating":6.1,
"IMDB_Votes":1071}


 */