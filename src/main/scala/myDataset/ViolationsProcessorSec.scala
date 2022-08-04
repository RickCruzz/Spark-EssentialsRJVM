package myDataset


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

/* This file serves only
to compile and test funcitons in the IntelliJ/Spark local cluster

SET The Violation File Location to a Sample or to the Full file.
 */


object ViolationsProcessorSec extends App {

  //val conf = new SparkConf().setMaster("172.21.0.1:7077/").setAppName("myApp")

  val spark = SparkSession.builder()
    .config("spark.master", "local[*]")
    .appName("Second Exploration")
    .config("spark.executor.memory", "1g")
    .config("spark.file.transferTo","false") // Forces to wait until buffer to write in the Disk, decrease I/O
    .config("spark.shuffle.file.buffer", "1m") // More buffer before writing
    .config("spark.io.compression.lz4.blockSize","512k")
    .getOrCreate()



def typeFile(bigFile: Boolean = false, sample: Boolean = false) = {
  if (bigFile) {
    // Corrupted File - 5,8GB
    // ("/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations_corrupted.csv" , "csv")
    // Original File - 22,1GB - Downloaded 29/07
    ("/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations.csv" , "csv")
  } else if (sample) {
    // parquet Sample
    ( "/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations/sample.parquet", "parquet")
  }else {
    // Silver File Generated
    ("/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations/silver.parquet", "parquet")
  }
}

  val (violationsFile, format) = typeFile() //return the SILVER if doesnt have parameters

  val violationDF = spark.read
    .format(format)
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .load(violationsFile)

  // STARTING USING ORC/PARQUET - OK
  // Try/Catch - ?
  // Handling errors with application - ?
  // % of data that is missing or problem - ?


  // Precinct File csv
  val precinctFile = "/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/precincts.csv"

  //Reading the precinctFile
  val precinctDF = spark.read
    .format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .option("sep",",")
    .load(precinctFile)

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  precinctDF.show()
  val numBeforeClean = violationDF.count()
  val cleanDF =  violationDF.na.drop(List("Plate","State","Issue_Date"))
  val numClean = cleanDF.count()
  val reduction = ((numClean.toDouble*100L)/numBeforeClean.toDouble)-100L
  println(s"We have original: ${numBeforeClean} rows and now we have: ${numClean} rows -> Reduced: ${reduction} %")


  /*
  ROWS ELIMINATED -> 306
  ROWS WITH ISSUE_DATE = NULL -> 20
  ROWS WITH PLATE = NULL -> 286
  ROWS WITH NO AMOUNT DECLARED (FINE_AMOUNT, PAYMENT_AMOUNT, AMOUNT_DUE) = NULL -> 4.712.767 -> 5% Dataset
   */

/*
/* 7846 - x
 */
  //Dataset Tests
  /*
  import spark.implicits._
  case class Violation(
                        Plate: Option[String],
                        State: Option[String],
                        License_Type: Option[String],
                        Summons_Number: Option[String],
                        Issue_Date: Option[String],
                        Violation_Time: Option[String],
                        Violation: Option[String],
                        Judgment_Entry_Date: Option[String],
                        Fine_Amount: Option[Double],
                        Penalty_Amount: Option[Double],
                        Interest_Amount: Option[Double],
                        Reduction_Amount: Option[Double],
                        Payment_Amount: Option[Double],
                        Amount_Due: Option[Double],
                        Precinct: Option[String],
                        County: Option[String],
                        Issuing_Agency: Option[String],
                        Violation_Status: Option[String],
                        Summons_Image: Option[String],

  //Conveting the DF to DS
  val viDS = violationDF.as[Violation]

  //Testing the same command results in two diferent formats.
  //First one with sparkSQL / DF
  spark.sql("select State, count(State) as sparkSQL from Violations group by State").show()
  //Dataset, Maping
  viDS.groupByKey(_.State).count().show()
*/

  def generateSilverFile(start:Boolean = false) = {
    if (start) {
      val columnsRedirected = violationDF.columns.mkString(",").replace(" ", "_").split(",").toSeq
      val viColumnsRenamedDF = violationDF.toDF(columnsRedirected: _*)
      viColumnsRenamedDF.repartition(25).write.mode(SaveMode.Append).save(s"${violationsFile.replace(".csv", "")}/silver.parquet")
    }
  }

  def generateSampleData (start:Boolean = false) = {
    if (start) {
      // Renaming The columns in the file, Changing the Space to _
      val columnsRedirected = violationDF.columns.mkString(",").replace(" ", "_").split(",").toSeq
      val viColumnsRenamedDF = violationDF.toDF(columnsRedirected: _*)

      // Taking a sample of 4 years
      // From 2022 until 2019
      val dfSample = viColumnsRenamedDF.select("*")
        .where("extract(YEAR from to_date(Issue_Date, 'MM/dd/yyyy')) >= 2019 and extract(YEAR from to_date(Issue_Date, 'MM/dd/yyyy')) <= 2022")
        .coalesce(6)
      dfSample.write.mode(SaveMode.Append).save(s"${violationsFile.replace(".csv", "")}/sample.parquet")
    }
  }

  //Setting this to true, the code shown above will generate an PARQUET Sample
  //The columns names will be changed from " " to "_"
  generateSilverFile(false)
  generateSampleData(false)



  //Creating Temporary view
  //violationDF.printSchema()
  //violationDF.createOrReplaceTempView("Violations")


  /*
  Check: Fine_Amount or Violation Null - Fine_Amount Null 4.712.767 (5,66%) - Violation Null -> 4.717.297 (5,67%)
         Blankplates Violations Total: 79.569 (0,01%) - 45.588 Null/Bad Records - No Payments: 31,225 - That have payment 2.756
         States Differents or Nulls. -> Have 70 Distinct States and  219.274 records from (STATE 99, 98 or Null) - 130.343 are bad records
         State: DP -> Us State Dept. - 159.325 | GV -> Government -> 72.322
         4.712.767 -> Unvalid Records 5,667%

         Issue_Date > 2022 - Total:  72.168 - Bad Records: 598 (0,09%) - Payed Tickets - 3.152 (4,37%)
         Issue_Date Null = 349
  */

 // Grouped Records by States
 //val states = violationDF.select("State")
 //.groupBy("State").agg(count("*").as("nRows")).orderBy(col("nRows").desc_nulls_last)
 //states.show(500, false)
  //println(violationDF.select("State", "Summons_Number", "Violation", "Fine_Amount").where( col("Fine_Amount").isNull).count())


  //violationDF.select("*").where((col("State") === "99") and col("Violation").isNotNull)
  violationDF.select("*").where((col("Plate") === "BLANKPLATE") and col("Fine_Amount").isNull)

  val years = violationDF.select("Issue_Date")
   .withColumn("Year", year(to_date(col("Issue_Date"), "MM/dd/yyyy"))).drop("Issue_Date").groupBy("Year").agg(count("*").as("NROWS")).sort(col("Year").desc_nulls_last)

  val year2DF = violationDF.withColumn("Year", year(to_date(col("Issue_Date"), "MM/dd/yyyy")))
    .withColumn("Date",to_date(col("Issue_Date"), "MM/dd/yyyy"))

  println(year2DF.where(col("Violation").isNull).count())
  println(year2DF.where(col("Fine_Amount").isNull).count())
  //  .coalesce(5).write.format("csv")
  //  .option("sep",",").option("header","true").mode(SaveMode.Overwrite)
  //  .save("/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations/silver.parquet/csvt")

  //years.show(500, false)

  //violationDF.groupBy("License_Type").agg(count("*").as("ntype")).orderBy(col("ntype").desc_nulls_last).show(500,false)
  //4717297
  //4712767
  //0004530
  val vDF = violationDF.withColumn( "Date", to_date(col("Issue_Date"), "MM/dd/yyyy"))
    .withColumn("Year", year(col("Date")))
    .where(col("Year") === 2096)


  //vDF.show(500, false)




  //Trying to identify Outliers
  // This DF contains data separated by Dates and Fields
  val viTimesDF = violationDF.select("Issue_Date", "Summons_Number")
    .withColumn( "Date", to_date(col("Issue_Date"), "MM/dd/yyyy"))
    .withColumn("Month", month(col("Date")))
    .withColumn("Day", dayofmonth(col("Date")))
    .withColumn("Year", year(col("Date")))
    .withColumn("Week", weekofyear(col("Date")))
    .withColumn("DayOfWeek", dayofweek(col("Date")))
    .withColumn("Quarter", quarter(col("Date")))
    .selectExpr("Date", "Month", "Day", "Year", "Week", "DayOfWeek","Quarter","Summons_Number")

  //Same as above but using SQL
  /* val viTimesDFSQL = spark.sql(
   * """select to_date(Issue_Date, 'MM/dd/yyyy') as Date,
   *            extract(MONTH from to_date(Issue_Date, 'MM/dd/yyyy')) as Month,
   *            extract(DAY from to_date(Issue_Date, 'MM/dd/yyyy')) as Day,
   *            extract(YEAR from to_date(Issue_Date, 'MM/dd/yyyy')) as Year,
   *            extract(WEEK from to_date(Issue_Date, 'MM/dd/yyyy')) as week,
   *            extract(DAYOFWEEK from to_date(Issue_Date, 'MM/dd/yyyy')) as DayOfWeek,
   *            extract(QUARTER from to_date(Issue_Date, 'MM/dd/yyyy')) as Quarter,
   *            Summons_Number
   * from Violations""")
   */

  // Reducing the number of partitions and persisting
  // viTimesDF.coalesce(20)
  // viTimesDF.persist()


  // AVG of Tickets per Day of week by Year
  val viDayWeekYearDF = viTimesDF.groupBy("Year", "DayOfWeek").agg(
    count("Summons_Number").as("N_Violations")
  ).withColumn("AVG_Summons", (col("N_Violations") / 52.1429).cast(DecimalType(18, 2)))
    .orderBy(col("N_Violations").desc_nulls_last)


  // Tickets per Days, weeks and months
  val countDF = viTimesDF.groupBy("Year", "Month", "Week", "DayOfWeek", "Date").agg(
    count("Summons_Number").as("N_Violations")
  ).orderBy(col("N_Violations").desc_nulls_last)

  // countDF.createOrReplaceTempView("Violations_DayWeek_Month_Year")
  // viDayWeekYearDF.createOrReplaceTempView("Violations_Dayweek_avg")

  // Now we can compare the AVG vs Number by day of week
  val condition = ((countDF.col("Year") === viDayWeekYearDF.col("Year")) and (countDF.col("DayOfWeek") === viDayWeekYearDF.col("DayOfWeek")))
  val joinedDF = countDF.join(viDayWeekYearDF, condition, "inner")
    .withColumn("Variation_Percent", round((((countDF.col("N_Violations") * 100) / viDayWeekYearDF.col("AVG_Summons")) - 100),4))
    .orderBy(col("Date").asc)




  //Same as above but using SQL
  /** spark.sql(
    * """select vio_complete.Date, vio_complete.Year, vio_complete.Week, vio_complete.DayOfWeek, vio_complete.Month,
    *           vio_complete.N_Violations, vio_avg.AVG_Summons,
    *          round((((vio_complete.N_Violations * 100) / vio_avg.AVG_Summons) - 100),4) as Variation_Percent,
    *          case when vio_complete.N_Violations > vio_avg.AVG_Summons then "Above AVG"
    *          when vio_complete.N_Violations < vio_avg.AVG_Summons then "Less than AVG"
    *          else "ERROR" end as Above_AVG
    *          from Violations_DayWeek_Month_Year as vio_complete
    *          left join Violations_Dayweek_avg as vio_avg
    *          on ((vio_avg.Year = vio_complete.Year) and (vio_avg.DayOfWeek = vio_complete.DayOfWeek))
    *          order by vio_complete.Date
    * """)
   */



  // -> PRECINT JOIN TO UNDERSTAND WHERE ARE THE HOTSPOTS
  // -> TIME OF MOST VIOLATIONS
*/
}
