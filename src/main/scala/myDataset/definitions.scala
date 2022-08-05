package myDataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, _}

object definitions {

  def schemaFile(): StructType = {
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
    return violationsSchema
  }

  def generateSilverFile(df: DataFrame, path: String = ""): String = {
    // Renaming The columns in the file, Changing the Space to _
    val columnsRedirected = df.columns.mkString(",").replace(" ", "_").split(",").toSeq
    val ColumnsRenamedDF = df.toDF(columnsRedirected: _*)

    //Removing NULLS
    val saveDF = ColumnsRenamedDF.na.drop(List("Plate","State","Issue_Date"))
      // Converting Date to a correct Format
      // Adding Fields that gonna be Reused on my Analyss
      .withColumn("Issue_Date", to_date(col("Issue_Date"), "MM/dd/yyyy"))
      .withColumn("Month", month(col("Issue_Date")))
      .withColumn("Day", dayofmonth(col("Issue_Date")))
      .withColumn("Year", year(col("Issue_Date")))
      .withColumn("Violation_Hour", when( length(col("Violation_Time")) >5 , concat(substring_index(col("Violation_Time"),":",1),lit(":00 - "), substring_index(col("Violation_Time"),":",1),lit(":59" ))).otherwise(null))
      .withColumn("Violation_Turn", when( length(col("Violation_Time")) >5 , when(col("Violation_Time").contains("A"),lit("AM")).otherwise(lit("PM"))).otherwise(null))
      //Drop Columns that are Un-functional
      .drop("Plate", "Violation_Time","Judgment_Entry_Date", "County", "Violation_Status", "Summons_Image")

    saveDF.repartition(25).write.mode(SaveMode.Append).save(s"${path.replace(".csv", "")}/silver.parquet")
  return (s"${path.replace(".csv", "")}/silver.parquet")
  }


  def generateSampleData (df: DataFrame, path: String = ""):String = {
   // Taking a sample of 4 years
   // From 2022 until 2019
   val sampleDF = df
     .where("extract(YEAR from Issue_Date) >= 2020 and extract(YEAR from Issue_Date) <= 2022")
     .coalesce(6)
    sampleDF.write.mode(SaveMode.Append).save(s"${path.replace(".csv", "")}/sample.parquet")
    return (s"${path.replace(".csv", "")}/sample.parquet")
  }





}

