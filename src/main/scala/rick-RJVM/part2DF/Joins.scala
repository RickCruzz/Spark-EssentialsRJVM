package rick.part2DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Joins extends App{

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()


  val guitarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/bands.json")


  //joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  //outerjoins
  //leftouterJoin = everything in the inner join + all the rows in the left table(guitarists) with nulls where have missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  //rightouterjoin = everything in the inner join + all the rows in the RIGHT table(Bands) with nulls where have missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // fullouterjoin = EVERYTHING
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // ANTI_JOIN
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")


  //Things to Remember
  //guitaristsBandsDF.select("id","band").show // this crashes

  //option1 - rename the column on wich we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id","band"),"band")

  //option2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  //option3 - rename the offending column and keep data
  val bandsModDF = bandsDF.withColumnRenamed("id","bandID")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandID"))

  //using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarID"), expr("array_contains(guitars,guitarID)"))


  // show all employees and their max_salaries
  /*
   1 -> Gerar um DF com max salaries x employee
   2 - Join Employee vs maxSalaryDf
   */
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()
  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .load()

  val maxSalariesDF = salariesDF.groupBy(col("emp_no"))
    .agg(
      max("salary").as("salary")
    )

  val employeeMaxSalDF = employeesDF.join(maxSalariesDF, "emp_no")
  employeeMaxSalDF.show()

  // show all employees who were never managers (left_anti)
  val deptManDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.dept_manager")
    .load()

  val empSimpleDF = employeesDF.join(deptManDF,
    deptManDF.col("emp_no") === employeesDF.col("emp_no"),
    "left_anti")
  empSimpleDF.show()

  // find the best job titles of the best paid 10 employees in the company
  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.titles")
    .load()

  /*
  select employee.first_name, employee.last_name, title from employee inner join salaries on (salaries.emp_no = employee.emp_no)
  inner join titles t on (t.emp_no = employee.emp_no)
  group by

   */
val titlesMaxDF = titlesDF.groupBy("emp_no").agg(
  max("from_date").as("from_date")
)

val titlesxEmpDF = titlesDF.join(titlesMaxDF, (titlesMaxDF.col("from_date") === titlesDF.col("from_date"))
  .and(titlesMaxDF.col("emp_no") === titlesDF.col("emp_no")), "semi")

//titlesxEmpDF.where("emp_no = 10030").show() -> Check

/*val salMaxDF = salariesDF.groupBy(col("emp_no"))
  .agg(
    max("from_date").as("from_date")
  )*/

  val salMaxDF = employeeMaxSalDF.orderBy(col("salary").desc).limit(10)

  val salxEmpDF = salariesDF.join(salMaxDF, (salMaxDF.col("from_date") === salariesDF.col("from_date"))
  .and(salMaxDF.col("emp_no") === salariesDF.col("emp_no")), "semi")
  salxEmpDF.show()
  //salxEmpDF.where("emp_no = 10030").show() -> check 2

  val salxTitleDF = titlesxEmpDF.join(salxEmpDF, salxEmpDF.col("emp_no") === titlesxEmpDF.col("emp_no"), "inner")
    .drop(salxEmpDF.col("emp_no"))

  val employeesTitlesSalDF = employeesDF.join(salxTitleDF, salxTitleDF.col("emp_no") === employeesDF.col("emp_no")).drop(salxTitleDF.col("emp_no") )
  employeesTitlesSalDF.show()


}
