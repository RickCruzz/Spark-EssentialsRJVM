package myDataset

import myDataset.definitions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}



object processor {

  def main(args: Array[String]) = {

    if (args.length != 2) {
      println(
        """Please define the correct Parameters
          |1 - Path to dataset.
          |2 - Type to create (Silver/Sample)
          |""".stripMargin)
      System.exit(1)
    }

    //spark-submit params
    //filepath / Type -> Silver/Sample / GenerateViews

    val spark = SparkSession.builder()
      .config("spark.master", "local[*]")
      .appName("Processor, Silver Layer or Sample File")
      //.config("spark.executor.memory", "1g")
      //.config("spark.file.transferTo","false") // Forces to wait until buffer to write in the Disk, decrease I/O
      //.config("spark.shuffle.file.buffer", "1m") // More buffer before writing
      //.config("spark.io.compression.lz4.blockSize","512k")
      .getOrCreate()

    // In case some of invalid Date, its gonna be fill with the next Correct Date
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    // In case some of invalid Date, its gonna be fill with NULL
    //spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    //val path = "/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations.csv"
    //val path = "/media/corujin/Coding/Applaudo/ScalaTraining/DataSet/Open_Parking_and_Camera_Violations/silverlayer.parquet"
    val path = args(0)
    val typeStage = args(1)


    try {
      val finalPath = typeStage.toUpperCase() match {
        case "SILVER" => generateSilverFile(
          spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(path)
          , path)
        case "SAMPLE" => generateSampleData(
          spark.read.parquet(path)
          , path)
        case _ => path
      }

      //Define decimalType and Schema
      val decimalType = DataTypes.createDecimalType(24, 2)
      val mainDF = spark.read.parquet(finalPath)
      val countRecords = mainDF.count()

      println(
        s"""The file was processed and have ${countRecords} Rows to be used on analysis.
           |Path of File for calculating Results: ${finalPath}
           |And this is the Schema for the file:
           |""".stripMargin)
      mainDF.printSchema()

  } catch{
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