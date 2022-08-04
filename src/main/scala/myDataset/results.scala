package myDataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

import java.io.FileNotFoundException

object results {

  def main(args: Array[String]) = {

    if (args.length < 1) {
      println(
        """Please define the correct Parameters
          |1 - Path to dataset (PARQUET_FILE/SAMPLE_FILE).
          |2 - Set 1 to Generate Hive Views. (OPTIONAL)
          |This file can be generated with processor.scala main method.
          |""".stripMargin)
      System.exit(1)
    }


    var views = "0"

    if (args.length == 2){
      views = args(1)
    }


    //val path = "/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations/silverlayer.parquet"

    val path = args(0)

    val spark = SparkSession.builder()
      .config("spark.master", "local[*]")
      .appName("Results")
      //.config("spark.executor.memory", "1g")
      //.config("spark.file.transferTo","false") // Forces to wait until buffer to write in the Disk, decrease I/O
      //.config("spark.shuffle.file.buffer", "1m") // More buffer before writing
      //.config("spark.io.compression.lz4.blockSize","512k")
      .getOrCreate()

    // In case some of invalid Date, its gonna be fill with the next Correct Date
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    val decimalType = DataTypes.createDecimalType(24, 2)


    try{
      //Reading the Parquet File
      val mainDF = spark.read.parquet(path)


      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Creating Insights
      // Insights
      // How Many Violations can be considered done by the Government.
      // Star_codes.pdf
      val countRecords = mainDF.count()
      println(s"Total of Records in the Dataset: ${countRecords} Rows")

      val govStates = Array("GV", "DP", "DC")
      val govDF = mainDF.where(
        upper(col("State")).isInCollection(govStates))
        .groupBy("State").agg(count("*").as("N_Records"))
        .withColumn("Percent_of_Records", ((col("N_Records") * 100L) / lit(countRecords)))

      govDF.show(10, false)


      //Peak Times of Violations
      val windowSpec = Window.partitionBy("Violation_Turn", "Year").orderBy(col("Num_Records").desc_nulls_last)

      val timeDF = mainDF.groupBy("Violation_Hour", "Violation_Turn", "Year").agg(
        count("Summons_Number").as("Num_Records"))

      timeDF.withColumn("row_number", row_number.over(windowSpec))
        .where(col("row_number") <= lit(3))
        .orderBy(col("Num_Records").desc_nulls_last, col("row_number").desc)
        .drop("row_number").show(20, false)


      //Wich State have more Violations. Payed Vs Open.
      //Payeds
      val payStateDF = mainDF.select(col("State"), col("Payment_Amount"))
        .where("Payment_Amount > 0").groupBy("State").agg(
        count("State").as("N_Records"),
        sum("Payment_Amount").as("Total_Received"),
        round(avg("Payment_Amount"), 2).as("Average_Received")
      )
      //Open tickets
      val dueStateDF = mainDF.select(col("State"), col("Amount_Due"))
        .where("Amount_Due > 0").groupBy("State").agg(
        count("State").as("N_Due_Records"),
        sum("Amount_Due").as("Total_Amount_Due"),
        round(avg("Amount_Due"), 2).as("Average_Amount_Due")
      ).withColumnRenamed("State", "Due_State")

      val payVsDueDF = payStateDF
        .join(dueStateDF, payStateDF.col("State") === dueStateDF.col("Due_State")
          , "outer").drop("Due_State").orderBy(col("Total_Received").desc)
      payVsDueDF.show(5)




      //Mean of Ticket Payed vs Standard Deviation
      val avgDF = mainDF.select(
        round(mean("Fine_Amount"), 2).as("Mean_Ticket"),
        round(stddev("Fine_Amount"), 2).as("STD_Ticket"))
      avgDF.show(false)


      //What is the most common violation that are payed by the users in the recent years?
      val violationYearDF = mainDF.where("Payment_Amount > 0").groupBy("Year", "Issuing_Agency", "Violation")
        .agg(count("Violation").as("N_Violations"))
        .orderBy(col("N_Violations").desc)

      violationYearDF.show(5, false)




      // This DF contains data separated by Dates and Fields
      // Trying to identify Outliers and discovery wich days/months are inside the mean of the year
      val viTimesDF = mainDF.select("Issue_Date", "Summons_Number", "Month", "Day", "Year")
        .withColumn("Week", weekofyear(col("Issue_Date")))
        .withColumn("DayOfWeek", dayofweek(col("Issue_Date")))


      // AVG of Tickets per Day of week by Year
      val viDayWeekYearDF = viTimesDF.groupBy("Year", "DayOfWeek").agg(
        count("Summons_Number").as("Total_Violations_Day")
      ).withColumn("AVG_Violations_Day", (col("Total_Violations_Day") / 52.1429).cast(DecimalType(18, 2)))

      // Tickets per Days, weeks and months
      val countDF = viTimesDF.groupBy("Year", "Month", "Week", "DayOfWeek", "Issue_Date").agg(
        count("Summons_Number").as("N_Violations")
      )

      // Now we can compare the AVG vs Number by day of week
      val condition = ((countDF.col("Year") === viDayWeekYearDF.col("Year"))
        and (countDF.col("DayOfWeek") === viDayWeekYearDF.col("DayOfWeek")))
      val joinedDF = countDF.alias("count").join(viDayWeekYearDF.alias("totals"), condition, "inner")
        .select("count.Year", "count.Month", "count.Week", "count.DayOfWeek", "count.Issue_Date", "count.N_Violations", "totals.Total_Violations_Day", "totals.AVG_Violations_Day")
        .withColumn("Variation_Percent",
          round((((countDF.col("N_Violations") * 100L) / viDayWeekYearDF.col("AVG_Violations_Day")) - 100L), 4))
        .orderBy(col("Issue_Date").asc)

      joinedDF.show(50, false)


      //Is the revenue generated by payed tickets increasing?
      val yearVsMonthDF = mainDF.withColumn("Year_Month",
        concat(col("Month"), lit('/'), col("Year")))
        .groupBy("Year_Month").agg(
        count("Year_Month").as("N_Violations"),
        sum(col("Fine_Amount").cast(decimalType)).as("Total_Fine"),
        sum(col("Payment_Amount").cast(decimalType)).as("Total_Received"),
        sum(col("Amount_Due")).cast(decimalType).as("Total_Amount_Due")
      ).orderBy(col("N_Violations").desc)

      yearVsMonthDF.show(false)


      if (views == "1") {
        val goldenPath = "/opt/spark-data/goldenLayer/"
        govDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_gov")
        timeDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_time")
        payVsDueDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_pay_due")
        avgDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_stats")
        violationYearDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_violation_year")
        joinedDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_avg_per_day")
        yearVsMonthDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_violations_year_month")
        println("Tables Created based on Results.")
      }

    }  catch{
      case ex: FileNotFoundException => {
        println(s"Check the File path you informed. File not found ${path}")
      }
      case ex: OutOfMemoryError =>{
        println("Check config params. OUT OF MEMORY!")
      }
      case ex: RuntimeException => {
        println(s"Run Time Exception")
      }
      case unknown: Exception => {
        println(s"Unknown exception: ${unknown}")
      }
    } finally {
      println("Process Finished.")
    }

  }


}
