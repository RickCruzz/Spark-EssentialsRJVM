package myDataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

/*
Testing functions and converting as DataSet

 */


object ViolationsProcessorSec extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .getOrCreate()

  val viDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .csv("spark-cluster/data/Open_Parking_and_Camera_Violations.csv")


  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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
                      )


  // Renaming The columns in the file, Changing the Space to _
  val columnsRedirected = viDF.columns.mkString(",").replace(" ", "_").split(",").toSeq
  val violationDF = viDF.toDF(columnsRedirected: _*)
  violationDF.createOrReplaceTempView("Violations")




  /*
  //Conveting the DF to DS
  val viDS = violationDF.as[Violation]

  //Testing the same command results in two diferent formats.
  //First one with sparkSQL / DF
  spark.sql("select State, count(State) as sparkSQL from Violations group by State").show()
  //Dataset, Maping
  viDS.groupByKey(_.State).count().show() val


   */

  //Trying to identify Outliers
  // This DF contains data separated by Dates and Fields
  val viTimesDF = spark.sql(
    """select to_date(Issue_Date, 'MM/dd/yyyy') as Date,
               extract(MONTH from to_date(Issue_Date, 'MM/dd/yyyy')) as Month,
               extract(DAY from to_date(Issue_Date, 'MM/dd/yyyy')) as Day,
               extract(YEAR from to_date(Issue_Date, 'MM/dd/yyyy')) as Year,
               extract(WEEK from to_date(Issue_Date, 'MM/dd/yyyy')) as week,
               extract(DAYOFWEEK from to_date(Issue_Date, 'MM/dd/yyyy')) as DayOfWeek,
               extract(QUARTER from to_date(Issue_Date, 'MM/dd/yyyy')) as Quarter,
               Summons_Number
    from Violations""")

  //Reducing the number of partitions
  viTimesDF.coalesce(20)
  viTimesDF.persist()

  // Counting total of violations by quarters
  viTimesDF.groupBy("Year", "Quarter").agg(
    count("Summons_Number").as("N_Violations")
  ).orderBy(col("N_Violations").desc_nulls_last).show(200, false)


  //Generating the AVG of Tickets per Day of week by Year
  val viDayWeekYearDF = viTimesDF.groupBy("Year", "DayOfWeek").agg(
    count("Summons_Number").as("N_Violations")
  ).withColumn("AVG_Summons", (col("N_Violations") / 52.1429).cast(DecimalType(18, 2)))
    .orderBy(col("N_Violations").desc_nulls_last)
  viDayWeekYearDF.createOrReplaceTempView("Violations_Dayweek_avg")

  //Showing Results into console
  viDayWeekYearDF.show(200, false)


  //Generating the view with Tickets per Days, weeks and months
  viTimesDF.groupBy("Year", "Month", "Week", "DayOfWeek").agg(
    count("Summons_Number").as("N_Violations")
  ).orderBy(col("N_Violations").desc_nulls_last).createOrReplaceTempView("Violations_DayWeek_Month_Year")


  // Now we can compare the AVG vs Number by day of week
  spark.sql(
    """select vio_complete.Year, vio_complete.Week, vio_complete.DayOfWeek, vio_complete.Month,
               vio_complete.N_Violations, vio_avg.AVG_Summons,
              case when vio_complete.N_Violations > vio_avg.AVG_Summons then "Above AVG"
              when vio_complete.N_Violations < vio_avg.AVG_Summons then "Less than AVG"
              else "ERROR" end as Above_AVG
              from Violations_DayWeek_Month_Year as vio_complete
              left join Violations_Dayweek_avg as vio_avg
              on ((vio_avg.Year = vio_complete.Year) and (vio_avg.DayOfWeek = vio_complete.DayOfWeek))
              where (vio_complete.Year = 2021)
              order by Above_AVG desc, vio_complete.N_Violations desc
  """).show(400, false)
  println(spark.sql(
    """select vio_complete.Year, vio_complete.Week, vio_complete.DayOfWeek, vio_complete.Month,
               vio_complete.N_Violations, vio_avg.AVG_Summons,
              case when vio_complete.N_Violations > vio_avg.AVG_Summons then "Above AVG"
              when vio_complete.N_Violations < vio_avg.AVG_Summons then "Less than AVG"
              else "ERROR" end as Above_AVG,
              case when vio_complete.N_Violations > vio_avg.AVG_Summons then 1
              when vio_complete.N_Violations < vio_avg.AVG_Summons then 0
              else 2 end as caseCompare
              from Violations_DayWeek_Month_Year as vio_complete
              left join Violations_Dayweek_avg as vio_avg
              on ((vio_avg.Year = vio_complete.Year) and (vio_avg.DayOfWeek = vio_complete.DayOfWeek))
              where (vio_complete.Year = 2021)
              order by caseCompare desc
  """).count())
}
