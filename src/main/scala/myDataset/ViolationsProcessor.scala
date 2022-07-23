package myDataset

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ViolationsProcessor {

  //Generic Method to Write in Table or CSV
  def writerJdbcDF(DFrame: DataFrame, table:String, clear: Boolean = false) {
    if (clear) {
      val driver = "org.postgresql.Driver"
      val url = "jdbc:postgresql://localhost:5432/rtjvm"
      val user = "docker"
      val pwd = "docker"

      DFrame.write
        .format("jdbc")
        .mode(SaveMode.Append)
        .options(Map(
          "driver" -> driver,
          "url" -> url,
          "user" -> user,
          "password" -> pwd,
          "dbtable" -> s"public.${table}"
        )).save
    }
  }

  def writeCSVDF(DFrame: DataFrame, path:String) {
    DFrame.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(path)
  }



  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need Input Path and Output Path")
      System.exit(1)
    }

    var clear = false
    if (args.length > 2) {
      var clear = args(2)
    }

    val spark = SparkSession.builder()
      .appName("Process Dataset")
      .config("spark.master","local[*]")
      .getOrCreate()

    val violationsFile = args(0)
    val exitPath = args(1)

    //Creating DecimalType
    val decimalType =  DataTypes.createDecimalType(24,2)

    //Defining Schema
    val violationsSchema = StructType(Array(
      StructField("Plate", StringType),
      StructField("State", StringType),
      StructField("License Type", StringType),
      StructField("Summons Number", StringType),
      StructField("Issue Date", DateType),
      StructField("Violation Time", StringType),
      StructField("Violation", StringType),
      StructField("Judgment Entry Date", StringType),
      StructField("Fine Amount", decimalType),
      StructField("Penalty Amount", decimalType),
      StructField("Interest Amount", decimalType),
      StructField("Reduction Amount", decimalType),
      StructField("Payment Amount", decimalType),
      StructField("Amount Due", decimalType),
      StructField("Precinct", StringType),
      StructField("County", StringType),
      StructField("Issuing Agency", StringType),
      StructField("Violation Status", StringType),
      StructField("Summons Image", StringType),
      StructField("_corrupt_record", StringType)))

    /*
    Plate,State,License Type,Summons Number,Issue Date,Violation Time,Violation,Judgment Entry Date,Fine Amount,Penalty Amount,Interest Amount,Reduction Amount,Payment Amount,Amount Due,Precinct,County,Issuing Agency,Violation Status,Summons Image
    JAL2979,NY,PAS,4664359196,08/28/2019,06:56P,PHTO SCHOOL ZN SPEED VIOLATION,,50.00,0.00,0.00,0.00,50.00,0.00,000,BX,DEPARTMENT OF TRANSPORTATION,,View Summons (http://nycserv.nyc.gov/NYCServWeb/ShowImage?searchID=VGtSWk1rNUVUVEZQVkVVMVRtYzlQUT09&locationName=_____________________)
     */

    // In case some of invalid Date, its gonna be fill with D+1
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


    val violationsDF = spark.read
      .schema(violationsSchema)
      .options(Map(
        "dateFormat" -> "MM/dd/yyyy",
        "mode"-> "PERMISSIVE",
        "enforceSchema" -> "true",
        "header" -> "true",
        "sep" -> ",",
        "nullValue" -> "")
      ).csv(violationsFile)

    //Checking How Many Partitions we Have
    //violationsDF.groupBy(spark_partition_id).count.show(50)


    //Amount Due Vs State
    //This Dataset shows wich State have most Violations, Open & Closed Revenues From all Data
    val aggDF = violationsDF.selectExpr(
      "State",
      "`Issue Date`",
      "Violation",
      "`Payment Amount`",
      "`Amount Due`",
      "`Issuing Agency`"
    ).coalesce(20).persist()



    val payStateDF = aggDF.select(col("State"), col("Payment Amount"))
      .where("`Payment Amount` > 0").groupBy("State").agg(
      count("State").as("N_Records"),
      sum("Payment Amount").as("Total_Received"),
      round(avg("Payment Amount"),2).as("Average_Received")
    )

    val dueStateDF = aggDF.select(col("State"), col("Amount Due"))
      .where("`Amount Due` > 0").groupBy("State").agg(
      count("State").as("N_Due_Records"),
      sum("Amount Due").as("Total_Amount_Due"),
      round(avg("Amount Due"),2).as("Average_Amount_Due")
    ).withColumnRenamed("State", "Due_State")

    val payVsDueDF = payStateDF.join(dueStateDF, payStateDF.col("State") === dueStateDF.col("Due_State"), "outer")
      .drop("Due_State")
      .withColumn("N_Due_Records", when(col("N_Due_Records").isNull, 0).otherwise(col("N_Due_Records")))
      .withColumn("Total_Amount_Due", when(col("Total_Amount_Due").isNull, 0).otherwise(col("Total_Amount_Due")))
      .withColumn("Average_Amount_Due", when(col("Average_Amount_Due").isNull, 0).otherwise(col("Average_Amount_Due")))
      .orderBy(col("Total_Received").desc)
    payVsDueDF.show(false)



    // The mean of Payed Amount in Violations VS The Standard Deviation
    // The Standard Deviation Shows the data are closer to the mean
    val avgDF = aggDF.select(
      round(mean("`Payment Amount`"),2).as("AVG_Ticket"),
      round(stddev("`Payment Amount`"),2).as("STD_Ticket")
    )
    avgDF.show(false)


    // Years / Month that have more Violations
    // This Data show us that we have a reduction of Violations between the two Months with the highest nuber of Violations in that Year
    // 08/2020 -> 882.039
    // 07/2021 -> 679.830

    val yearVsMonthDF = aggDF
      .withColumn("Year_Month", concat(month(col("`Issue Date`")),lit('/'),year(col("`Issue Date`")) ) )
      .groupBy("Year_Month").agg(
      count("Year_Month").as("N_Violations"),
      sum("Payment Amount").as("Total_Received"),
      sum("Amount Due").as("Total_Amount_Due")
    ).orderBy(col("N_Violations").desc)
    yearVsMonthDF.show(false)


    // Issuing Agency x Year x Violation That has been Payed
    // The Most Violations Issued are PHTO SCHOOL ZN SPEED VIOLATION
    // This Data shows that the School Zones are needing reinforcement of officers

    val violationYearDF = aggDF.selectExpr(
      "year(`Issue Date`) as Year",
      "`Issuing Agency`",
      "Violation"
    ).where("`Payment Amount` > 0").groupBy("Year", "`Issuing Agency`", "Violation").agg(
      count("Violation").as("N_Violations")
    ).orderBy(col("N_Violations").desc)

    violationYearDF.show(false)


    writeCSVDF(payVsDueDF, s"${exitPath}/Pay_Due")
    writeCSVDF(avgDF, s"${exitPath}/Mean_StdDev")
    writeCSVDF(yearVsMonthDF, s"${exitPath}/Violations_MonthNYear")
    writeCSVDF(violationYearDF, s"${exitPath}/Agency_Year_Violation")

    writerJdbcDF(payVsDueDF, "violation_year", clear)
    writerJdbcDF(avgDF, "violation_avg_stdev", clear)
    writerJdbcDF(yearVsMonthDF, "violation_year_month_revenue", clear)
    writerJdbcDF(violationYearDF, "violation_issue_year", clear)
  }
}


