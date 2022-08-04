package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._


object TaxiApp extends App{

  /* Test
    val spark = SparkSession.builder()
    .config("spark.master", "spark://172.21.0.1:7077")
    //.master("spark://0d1d8e86f1ab:7077")
    .appName("Second Exploration")
    .config("spark.sql.shuffle.partitions",5)
    .config("spark.executor.memory", "1g")
    .config("spark.file.transferTo","false") // Forces to wait until buffer to write in the Disk, decrease I/O
    .config("spark.shuffle.file.buffer", "1m") // More buffer before writing
    .config("spark.io.compression.lz4.blockSize","512k")
    .getOrCreate()
   */

  val spark = SparkSession.builder()
    .config("spark.master","local[*]")
    .appName("Taxi big data App")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC+2") // Needed to set th so I can get the same results as video


  val taxiZonesDF = spark.read.option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()
  println(taxiDF.count)

  taxiZonesDF.printSchema()

  /* Questions to Answer
   * 1 - Wich zones have most pickups/dropoffs overall?
   * 2 - What are the peak hours for taxi
   * 3 - How are the trips distributed by lenght? Why are people taking the cab?
   * 4 - What are the Peak hours for long/short Trips?
   * 5 - What are the top 3 pickup/dropoff zones for log/short trips?
   * 6 - How are people paying for the ride, on long/short trips?
   * 7 - How is the payment type evolving with time?
   * 8 - Can we explore a ride-sharing opportunity by grouping close short trip?
   */

  // 1 -

  val pickGroupedDF = taxiDF.groupBy("PULocationID").agg(
    count("*").as("TotalTrips")
  ).join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("service_zone", "LocationID")
    .orderBy(col("TotalTrips").desc_nulls_last)

  pickGroupedDF.show()

  val pickBoroughDF = pickGroupedDF.groupBy("Borough").agg(
    sum("TotalTrips").as("TotalTrips")
  ).orderBy(col("TotalTrips").desc_nulls_last)

  pickBoroughDF.show()


  //2
  val peakHoursDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)

  peakHoursDF.show()

  //3
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30

  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )

  tripDistanceStatsDF.show(false)

  val tripsWithLenghtDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLenghtDF = tripsWithLenghtDF.groupBy("isLong").agg(
    count("*").as("TotalTrips")
  )
  tripsByLenghtDF.show()

  //4
  val peakHoursByLenghtDF = tripsWithLenghtDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)
  peakHoursByLenghtDF.show(48, false)

  //5
  def pickupsDropoffPopularity(predicate: Column ) = tripsWithLenghtDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID")
    .agg(count("*").as("TotalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone","Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone","Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("TotalTrips").desc_nulls_last)

  pickupsDropoffPopularity(col("isLong")).show(false)
  pickupsDropoffPopularity(not(col("isLong"))).show(false)

  // 6
  val rateCodeDistributionDF = taxiDF.groupBy(col("RatecodeID")).agg(
    count("*").as("TotalTrips")
  ).orderBy(col("TotalTrips").desc_nulls_last)

  // 1 -> Credit Card  | 2 -> Cash  | 3 -> No charge | 4 -> Dispute | 6 -> Voided | 99 - ???
  rateCodeDistributionDF.show(false)


  // 7
  val rateCodeEvolutionDF = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("pickup_day").desc_nulls_last)


  //8
  val passengerCountDF = taxiDF.where(col("passenger_count") < 3 ).select(count("*"))
  passengerCountDF.show()
  taxiDF.select(count("*")).show()

  val groupAttemptsDF = taxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime"))/300).cast("integer").as("fiveMinId"), col("PULocationID"), col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID")).agg(
    count("*").as("TotalTrips"),
    sum(col("total_amount")).as("total_amount")
    ).orderBy(col("TotalTrips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinID")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  groupAttemptsDF.show(false)

  import spark.implicits._


  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extracost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimatedEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("TotalTrips") * percentGroupAttempt)
    .withColumn("acceptedGroupRidesImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extracost)
    .withColumn("totalImpact", col("acceptedGroupRidesImpact") + col("rejectedGroupedRidesImpact"))

  groupingEstimatedEconomicImpactDF.show(100,false)

  val totalImpactDF = groupingEstimatedEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  totalImpactDF.show(false)

}
