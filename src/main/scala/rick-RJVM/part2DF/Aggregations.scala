package rick.part2DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App{

  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")


  //Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all values not nulls
  moviesDF.selectExpr("count(Major_Genre)")

  //Counting all
  moviesDF.select(count("*")) // count all rows, and include nulls

  //Counting Distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // Approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // Min and Max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  //Sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  //AVG
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // Data Science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )


  //Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // include nulls
    .count() // select count(*) from movieDF group by Major Genre

  val averageRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("AVG_Rating")
    )
    .orderBy(col("AVG_Rating"))


  aggregationsByGenre.show()

  //1
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross")).show()

  moviesDF.selectExpr(
    "sum(US_Gross+Worldwide_Gross+US_DVD_Sales)"
  ).show()

  //2
  moviesDF.select(countDistinct("Director")).show()

 //How many movies Each director made
  val directorsDF = moviesDF
    .groupBy(col("Director"))
    .agg(
      count("*").as("N_Directors")
    )

  directorsDF.show()


  //3
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  val meanDevDF = moviesDF.selectExpr(
    "mean(US_Gross)",
    "stddev(US_Gross)"
  )

  meanDevDF.show()


  val avgImdbGrossDF = moviesDF.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("AVG_IMDB"),
      avg("US_Gross").as("AVG_GROSS")
    ).orderBy(col("AVG_IMDB").desc_nulls_last)

  avgImdbGrossDF.show()

  /*
     val daysDF = violationsDF.withColumn("Month", date_format(col("Issue_Date"), "MMM"))
     .withColumn("Day", date_format(col("Issue_Date"), "dd"))
     .groupBy("Day", "Month").agg(
     count("Day").as("N_Violations")
   ).orderBy(col("N_Violations").desc)
   daysDF.show()
   ]


   val yearVsMonthDF = violationsDF.withColumn("Year_Month", month(col("Issue_Date")))
     .withColumn("Day", dayofmonth(col("Issue_Date")))
     .groupBy("Day", "Month").agg(
     count("Day").as("N_Violations")
   ).orderBy(col("N_Violations").desc)
   daysDF.show(false)

    */

  /*
    val violationsSchema = StructType(Array(
    StructField("Plate", StringType),
    StructField("State", StringType),
    StructField("License_Type", StringType),
    StructField("Summons_Number", StringType),
    StructField("Issue_Date", DateType),
    StructField("Violation_Time", StringType),
    StructField("Violation", StringType),
    StructField("Judgment_Entry_Date", StringType),
    StructField("Fine_Amount", DoubleType),
    StructField("Penalty_Amount", DoubleType),
    StructField("Interest_Amount", DoubleType),
    StructField("Reduction_Amount", DoubleType),
    StructField("Payment_Amount", DoubleType),
    StructField("Amount_Due", DoubleType),
    StructField("Precinct", StringType),
    StructField("County", StringType),
    StructField("Issuing_Agency", StringType),
    StructField("Violation_Status", StringType),
    StructField("Summons_Image", StringType)))
   */

}


