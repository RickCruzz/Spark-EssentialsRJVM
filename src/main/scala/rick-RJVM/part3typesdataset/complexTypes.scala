package rick.part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object complexTypes extends App{


  val spark = SparkSession.builder()
    .appName("Complex DataTypes")
    .config("spark.master","local")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  //Dates
  val moviesWithReleaseDates = moviesDF.select(col("Title"), col("Release_Date"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDates.withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release"))/365)







  val stockCSV = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/stocks.csv")

  stockCSV.printSchema()
  val stockDateDF = stockCSV.withColumn("Date_Formatted", to_date(col("date"), "MMM dd yyyy"))
  stockDateDF.show()


  //Structures

  // 1-  with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"),col("Worldwide_Gross")).as("Profit"))
    .show()

  //2 - with expr
  moviesDF.selectExpr("Title","(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_gross")


  // Arrays
  val moviesWithWordsDF = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY OF STRINGS

  moviesWithWordsDF.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love"))
      .show()




}
