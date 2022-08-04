package Book

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Chapter7 extends App{

  val spark = SparkSession.builder()
    .config("spark.sql.shuffle.partitions",5)
    .config("spark.executor.memory", "2g")
    .config("spark.master","local[*]")
//  .master("local")
    .config("spark.file.transferTo","false") // Forces to wait until buffer to write in the Disk, decrease I/O
    .config("spark.shuffle.file.buffer", "1m") // More buffer before writing
    .config("spark.io.compression.lz4.blockSize","512k") // Increasing the compressed size of block, decreases the size of shuffle file
    .appName("testingConfig")
    .getOrCreate()


  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }

  printConfigs(spark)
  spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
  println(" ****** Setting Shuffle Partitions to Default Parallelism")
  // spark.sql.files.maxPartitionBytes //Default 128MB -> Configures the size of a partition
  printConfigs(spark)

  /*
  On Reading files can use .repartition(16) to set partitions
  * */

  val df = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
  df.cache() // Cache the data
  println(df.count()) // Materialize the cache
  println(df.count()) // Should be FASTER Than the first




  /* By Default this is False
  spark.dynamicAllocation.enabled true
  spark.dynamicAllocation.minExecutors 2
  spark.dynamicAllocation.schedulerBacklogTimeout 1m
  spark.dynamicAllocation.maxExecutors 20
  spark.dynamicAllocation.executorIdleTimeout 2min
  */

}
