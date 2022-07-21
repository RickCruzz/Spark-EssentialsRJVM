package rick.part3typesdataset

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object DataSets extends App{

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master","local")
    .getOrCreate()



  val numbersDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header","true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()


  //convert dataframe to a DS
  implicit val intEnconder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ <100)



  //dataset of a complex type
  // 1 - Define your case class

  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Option[Long],
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

  // 2 - read From the file
  def readDF(filename: String) = spark.read.option("inferSchema", "true").json(s"src/main/resources/data/${filename}")

  // 3 - Define an encoder (import spark.implicits._
  import spark.implicits._
  val carsDF = readDF("cars.json")
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_<100)

  //map, flatMap, fold, reduce, for comprehensions...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

  val carsCount = carsDS.count
  println(carsCount)
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  //also use DF Functions
  carsDS.select(avg(col("Horsepower")))


// Joins
  case class Guitars(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer( id: Long, name:String, guitars: Seq[Long], band:Long)
  case class Band (id: Long, name: String, hometown: String, year:Long)

  val guitarsDS = readDF("guitars.json").as[Guitars]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayersBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayersBandsDS.show


  val guitarsPlayersDS = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"),guitarsDS.col("id")), "outer")
  guitarsPlayersDS.show


  //Grouping Datasets
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE Transformations - WILL ENVOLVE SHUFFLE TRANSFORMATIONS


}
