package Book

import org.apache.spark.sql.SparkSession
import scala.util.Random._
import org.apache.spark.sql.functions._



object Chapter6 extends App {


  val spark = SparkSession.builder()
    .appName("Chapter 6")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  // Case Class for the Dataset
  case class Usage(uid: Int, uname: String, usage: Int)

  val r = new scala.util.Random(42)
  //Create 1000 Instances of the scala Usage Class
  val data = for (i <- 0 to 1000)
    yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

  //create a dataset of Usage Typed Data
  val dsUsage = {
    spark.createDataset(data)
  }
  dsUsage.show(10)

  dsUsage
    .filter(d => d.usage > 900)
    .orderBy(desc("usage"))
    .show(5, false)

  // Create a new case class with an additional field, cost
  case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)
  // Compute the usage cost with Usage as a parameter
  // Return a new object, UsageCost
  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
    UsageCost(u.uid, u.uname, u.usage, v)
  }
  // Use map() on our original Dataset
  dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

}
