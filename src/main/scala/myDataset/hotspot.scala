package myDataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.FileNotFoundException

object hotspot {

  def main(args: Array[String]) = {

   if (args.length < 2) {
     println(
       """Please define the correct Parameters
         |1 - Path to dataset (PARQUET_FILE/SAMPLE_FILE).
         |2 - Path to precincts (CSV_FILE).
         |The first parameter file can be generated with processor.scala main method.
         |""".stripMargin)
     System.exit(1)
   }

    val mainPath = args(0)
    val precinctPath = args(1)

    //val mainPath = "/media/corujin/Coding/Applaudo/ScalaTraining/Sharepoint Courses/Spark Essentials/spark-essentials-master/spark-cluster/data/Open_Parking_and_Camera_Violations/silver.parquet"
    //val precinctPath = "/media/corujin/Coding/Applaudo/ScalaTraining/Sharepoint Courses/Spark Essentials/spark-essentials-master/spark-cluster/data/precincts.csv"

    val spark = SparkSession.builder()
      .config("spark.master", "local[*]")
      .appName("Results Precincts")
      .getOrCreate()

    try{
      // Reading the Parquet File
      val mainDF = spark.read.parquet(mainPath)
      val precinctDF = spark.read.option("header", "true").option("inferSchema","true").csv(precinctPath)

      // Joining and Identifying the Hot Spot by Boroughs
      val condition = (mainDF.col("Precinct") === precinctDF.col("PrecinctNumber"))
      val hotSpotDF = mainDF.join(precinctDF, condition, "inner")
        .groupBy("Year", "PrecinctName", "Borough")
        .agg(count("*").alias("N_Occurrences"))
        .orderBy(col("N_Occurrences").desc_nulls_last)

      // exporting the dataframe
      hotSpotDF.show(50,false)
      val goldenPath = "/opt/spark-data/goldenLayer/"
      hotSpotDF.coalesce(1).write.mode("overwrite").parquet(goldenPath + "results_hot_spot")

    }  catch{
      case ex: FileNotFoundException => {
        println(s"Check the File path you informed. File not found ${mainPath}")
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
