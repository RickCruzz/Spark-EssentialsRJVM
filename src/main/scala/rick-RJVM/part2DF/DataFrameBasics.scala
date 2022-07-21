package rick.part2DF

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameBasics extends App{

  //Creating Session
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  //Reading a DF

  val firstDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  //showing a DF
  firstDF.show()

  //printSchema
  firstDF.printSchema()

  //Foreach row print the array of data
  //get rows
  firstDF.take(10).foreach(println)

  //spark types
  val longType = LongType

  //schema
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", IntegerType),
      StructField("Cylinders", IntegerType),
      StructField("Displacement", IntegerType),
      StructField("Horsepower", IntegerType),
      StructField("Weight_in_lbs", IntegerType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  //obtain a schema
  val carsDFSchema = firstDF.schema

  //read DF with my schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  //createRows by hand
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  //Create DF from touples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

   val manualCarsDF = spark.createDataFrame(cars) //schema-auto-inferred

  //DFS have schemas ROWS NOT

  //Create DFs with implicits
  import spark.implicits._
  val manualCarsDFImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDF.printSchema()
  manualCarsDFImplicits.printSchema()

   /**
     * 1) Create Manual DF Describing smartphones
     *  - make
     *  - model
     *  - screen dimension
     *  - RAM
     *  - camera mpx
     *  - YEAR RELEASE
     *
    **/
     val phones = Seq(
    ("Apple","Iphone 6",  4.7, 8,1,"2014-01-01","USA"),
    ("Apple","Iphone 6s", 4.7, 12,2,"2015-01-01","USA"),
    ("Apple","Iphone 7",  4.7, 12,2,"2016-01-01","USA"),
    ("Apple","Iphone 12", 6.1, 24,4,"2022-01-01","USA"),
    ("Apple","Iphone SE", 4.7, 12,4,"2022-01-01","USA")
    )
    val phonesDF = phones.toDF("Make", "Model", "Screen Dimension", "CameraMegapixels", "RAM", "Year", "Country")
  phonesDF.show()
  phonesDF.printSchema()


    /**
     * 2) Read Another File from the data Folder
     * movies.json
     * printSchema
     * count number of rows
     *
     *
     * */

    val moviesDF = spark.read
      .format("json")
      .option("inferSchema","true")
      .load("src/main/resources/data/movies.json")

  moviesDF.show(5)
  moviesDF.printSchema()
  println(moviesDF.count())

}
