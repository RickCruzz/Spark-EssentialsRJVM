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
      StructField("License_Type", StringType),
      StructField("Summons_Number", StringType),
      StructField("Issue_Date", DateType),
      StructField("Violation_Time", StringType),
      StructField("Violation", StringType),
      StructField("Judgment_Entry_Date", StringType),
      StructField("Fine_Amount", decimalType),
      StructField("Penalty_Amount", decimalType),
      StructField("Interest_Amount", decimalType),
      StructField("Reduction_Amount", decimalType),
      StructField("Payment_Amount", decimalType),
      StructField("Amount_Due", decimalType),
      StructField("Precinct", StringType),
      StructField("County", StringType),
      StructField("Issuing_Agency", StringType),
      StructField("Violation_Status", StringType),
      StructField("Summons_Image", StringType)))
    return violationsSchema
  }

  def fieldsDF (df:DataFrame):DataFrame= {
    // Converting Date to a correct Format
    // Adding Fields that gonna be Reused on my Analyss
    val fieldsDF = df.withColumn("Issue_Date", to_date(col("Issue_Date"), "MM/dd/yyyy"))
      .withColumn("Month", month(col("Issue_Date")))
      .withColumn("Day", dayofmonth(col("Issue_Date")))
      .withColumn("Year", year(col("Issue_Date")))
      .withColumn("HourStart", when(regexp_extract(col("Violation_Time"),"""[0-9]+""",0).cast(IntegerType) <= 24, regexp_extract(col("Violation_Time"),"""[0-9]+""",0)).otherwise(null))
      .withColumn("Violation_Hour", when(col("HourStart") > 12, (col("HourStart") - 12).cast(IntegerType)).otherwise(col("HourStart")))
      .withColumn("Violation_Hour", when(length(col("Violation_Hour")) === 1, concat(lit("0"), col("Violation_Hour"))).otherwise(col("Violation_Hour")))
      .withColumn("Violation_Hour", when(col("Violation_Hour").isNotNull, concat(col("Violation_Hour"),lit(":00 - "), col("Violation_Hour"),lit(":59" ))).otherwise(null))
      .withColumn("TurnBased", when(length(regexp_extract(upper(col("Violation_Time")),"""[A-Z]+""",0)) === 1,
        concat(regexp_extract(upper(col("Violation_Time")),"""[A-Z]+""",0),lit("M"))).otherwise(null))
      .withColumn("Violation_Turn", when(col("TurnBased").isNotNull, col("TurnBased"))
        .otherwise(
          when(col("HourStart") > 12 and col("TurnBased").isNull , "PM")
            .when(col("HourStart") <= 12 and col("TurnBased").isNull, "AM").otherwise(null)
        )
      )
      .withColumn("Violation_Turn", when(col("Violation_Hour").isNotNull, col("Violation_Turn")).otherwise(null))
    //.withColumn("Violation_Hour", when( length(col("Violation_Time")) >5 , concat(substring_index(col("Violation_Time"),":",1),lit(":00 - "), substring_index(col("Violation_Time"),":",1),lit(":59" ))).otherwise(null))
    //.withColumn("Violation_Turn", when( length(col("Violation_Time")) >5 , when(col("Violation_Time").contains("A"),lit("AM")).otherwise(lit("PM"))).otherwise(null))
    return fieldsDF
  }

  def generateSilverFile(df: DataFrame, path: String = ""): String = {

    // Renaming The columns in the file, Changing the Space to _
    // val columnsRedirected = df.columns.mkString(",").replace(" ", "_").split(",").toSeq
    // val ColumnsRenamedDF = df.toDF(columnsRedirected: _*)
    val saveDF = fieldsDF(df).
        na.drop(List("Plate","State","Issue_Date")) //Removing NULLS
        //Drop Columns that are Un-functional
        .drop("Plate", "Violation_Time","Judgment_Entry_Date", "County", "Violation_Status", "Summons_Image","HourStart", "TurnBased")

    saveDF.printSchema()
    saveDF.write.mode(SaveMode.Overwrite).save(s"${path.replace(".csv", "")}/silver.parquet")
  return (s"${path.replace(".csv", "")}/silver.parquet")
  }



  def generateSampleDataByYear (df: DataFrame, path: String = ""):String = {
   // Taking a sample of 4 years
   // From 2022 until 2019
   val sampleDF = fieldsDF(df)
     .where("extract(YEAR from Issue_Date) >= 2020 and extract(YEAR from Issue_Date) <= 2022")
     .na.drop(List("Plate","State","Issue_Date"))
     .drop("Plate", "Violation_Time","Judgment_Entry_Date", "County", "Violation_Status", "Summons_Image","HourStart", "TurnBased")
    sampleDF.write.mode(SaveMode.Overwrite).save(s"${path.replace(".csv", "")}/sampleYear.parquet")
    return (s"${path.replace(".csv", "")}/sampleYear.parquet")
  }

  def generateSampleData (df: DataFrame, path: String = ""):String = {
    //Sample of 0.3% of the main dataset
    val sampleDF = df.sample(0.3)
    val finalDF = fieldsDF(sampleDF)
      .na.drop(List("Plate","State","Issue_Date"))
      .drop("Plate", "Violation_Time","Judgment_Entry_Date", "County", "Violation_Status", "Summons_Image","HourStart", "TurnBased")
    finalDF.write.mode(SaveMode.Overwrite).save(s"${path.replace(".csv", "")}/sample.parquet")
    return (s"${path.replace(".csv", "")}/sample.parquet")
  }






}

