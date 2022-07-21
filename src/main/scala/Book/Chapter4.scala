package Book

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Chapter4 extends App{

val spark = SparkSession.builder
  .appName("SparkSQLExampleApp")
  .config("spark.master","local")
  .getOrCreate()

  val csvFile = "src/main/resources/book/departuredelays.csv"

  val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
  val df = spark.read.format("csv")
    .schema(schema)
    .option("header", "true")
    .load(csvFile)
  // Create a temporary view
  df.createOrReplaceTempView("us_delay_flights_tbl")

  spark.sql("""SELECT distance, origin, destination
  FROM us_delay_flights_tbl WHERE distance > 1000
  ORDER BY distance DESC""").show(10)

  (df.select("distance", "origin", "destination")
    .where("distance > 1000")
    .orderBy(col("distance").desc).show(10))

  spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)


  df.select("date","delay","origin","destination")
    .where("delay > 120 and origin = 'SFO' and destination = 'ORD'")
    .orderBy(col("delay").desc).show(10)



  spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)


  //Create DATABASE
  spark.sql("CREATE DATABASE learn_spark_db")
  spark.sql("USE learn_spark_db")

  //CREATE MANAGED TABLE -> WHEN DROP EVERYTHING GOES AWAY
  spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

  //CREATE UNMANAGED TABLE -> WHEN DROP LOSES ONLY METADATA
  spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
 distance INT, origin STRING, destination STRING)
 USING csv OPTIONS (PATH
 'src/main/resources/book/departuredelays.csv')""")
  /*
  (flights_df
 .write
 .option("path", "/tmp/data/us_flights_delay")
 .saveAsTable("us_delay_flights_tbl"))
   */



  val df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
  val df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

  df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
  df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

  spark.catalog.listDatabases()
  spark.catalog.listTables()
  spark.catalog.listColumns("us_delay_flights_tbl")
}
