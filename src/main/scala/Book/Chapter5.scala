package Book

import org.apache.spark.sql.SparkSession

object Chapter5 extends App{

  val spark = SparkSession.builder()
    .appName("Chapter5")
    .config("spark.master","local")
    .getOrCreate()

  val cubed = (s:Long) => {
  s*s*s
  }

  spark.udf.register("cubed", cubed)
  spark.range(1,9).createOrReplaceTempView("udf_test")

  spark.sql("Select id, cubed(id) as id_cubed from udf_test").show()

}
