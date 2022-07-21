package rick.part2DF

import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataSources extends App{

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master","local")
    .getOrCreate()

  val carsSchema = StructType(Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", IntegerType),
      StructField("Cylinders", IntegerType),
      StructField("Displacement", IntegerType),
      StructField("Horsepower", IntegerType),
      StructField("Weight_in_lbs", IntegerType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    ))

  /*
  * Reading DF:
  * - Format
  * - path
  * - Schema or inferSchema = true
  * - option: zero or more options
  * */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) //enforcing schema
    //.option("mode","failFast") // dropMalformed, permissive (default)
    .option("path","src/main/resources/data/cars.json")
    .load()
  carsDF.show()

  //alternative with options Map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()
  carsDFWithOptionMap.show()

  //Writing DFs
  // - format
  // - save mode (overwrite, append, ignore, errorIfExists
  // - path
  // - zero or more options
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe_rick.json")


  //Json Flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema: if spark fails parsings will put NULL
    .option("allowSingleQuotes","true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stockDF = spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  //Parquet
  //default mode of saving
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_rick_dupe.parquet")


  //TextFiles
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //reading from remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /*
  Exercise: Read the movies DF, then write as
  - tab-separted values file (CSV)
  - snappy parquet
  - table public.movies in the Postgres DB
   */

  val moviesDF  = spark.read
    .option("dateFormat", "dd-MMM-YY")
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  moviesDF.show()

  //salvando em CSV
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies_csv.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies_parquet.parquet")

  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" ->  "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker",
    "dbtable" -> "public.movies"
    )).save




}
