package rick.part2DF

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  var firstColumn = carsDF.col("Name")

  // Selecting (Projection)
  val carNamesDF = carsDF.select(firstColumn)

  //Various Select Methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, //Scala Symbol
    $"Horsepower", // Fancier interpolated string, return a Column Object
    expr("Origin") //EXPRESSION
  )

  //select with plain column Names
  carsDF.select("Name","Year") //Another DF

  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kgs"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2")
  )

  //SelectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //DF Processing
  //add a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs")/2.2)

  //rename a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  //carefull with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  //REMOVE
  carsWithColumnRenamed.drop("Cylinders","Displacement")


  //FILTERING
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  //filtering with expression
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  //Chain Filters
  val americanPowerFullCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerFullCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerFullCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //Unioning = add more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")


  val allCarsDF = carsDF.union(moreCarsDF) //works if the DFs have the same schema

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()


  /**
    *
    * 1. READ MOVIES DF and select 2 Columns of your choice (Title, Release Date)
    * 2. Create column: Sums : us_gross, worldwide_gross, DVD_Sales
    * 3. Select Comedy movies Major Genre with IMDB_Rating > 6
    *
    * Use as many version as possible
    */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  //1
  val moviesTitleReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesTitleRelease2DF = moviesDF.select(col("Title"), col("Release_Date"))
  val moviesTitleRelease3DF = moviesDF.select(moviesDF.col("Title"), moviesDF.col("Release_Date"))
  moviesTitleReleaseDF.show()

  //2
  val moviesRevenueDF = moviesDF.selectExpr(
    "Title",
    "Release_Date",
    "IFNULL(US_Gross,0) as US_Gross",
    "IFNULL(Worldwide_Gross,0) as Worldwide_Gross",
    "IFNULL(US_DVD_Sales,0) as US_DVD_Sales",
    "IFNULL(US_Gross,0) + IFNULL(Worldwide_Gross,0) + IFNULL(US_DVD_Sales,0) as Total_Revenue"
  )

  moviesRevenueDF.show()

  val moviesRevenue2DF = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Revenue",     expr("IFNULL(US_Gross,0) + IFNULL(Worldwide_Gross,0) + IFNULL(US_DVD_Sales,0) as Total_Revenue"))
  moviesRevenue2DF.show()

  //Select Comedy movies Major Genre with IMDB_Rating > 6

  val comedyMoviesDF = moviesDF.select($"Title", $"Release_Date", $"Major_Genre", $"IMDB_Rating", $"Rotten_Tomatoes_Rating")
    .where("IMDB_Rating > 6 and Rotten_Tomatoes_Rating > 60 and Major_Genre='Comedy'")
  comedyMoviesDF.show()

  val comedyMoviesDF2 = moviesDF.select($"Title", $"Release_Date", $"Major_Genre", $"IMDB_Rating", $"Rotten_Tomatoes_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
    //.where(col("Major_Genre") === "Comedy").where(col("IMDB_Rating") > 6)
  comedyMoviesDF2.show()
}
