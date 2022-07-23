package part4sql


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App{
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master","local[*]")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
//  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") // REMOVED IN THE VERSION 3.0.0
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")


  //select in DF
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  //selecting with SQL

  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      select Name from cars where Origin = 'USA'
      """)

  americanCarsDF.show()


  //we can run any SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  databasesDF.show()


  //Transfer tables from a DB to Spark Tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.${tableName}")
    .load()

  def transferTables(tableNames: List[String]) = tableNames.foreach { tableName =>

    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
//    tableDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  }



  transferTables(List(
    "departments",
    "dept_emp",
    "dept_manager",
    "employees",
    "movies",
    "salaries",
    "titles"))


  //read from the Warehouse

  val employeesDF2 = spark.read.table("employees")
  employeesDF2.show()


  val moviesWarehouseDF = spark.read.table("movies")
  moviesWarehouseDF.show()


  //1 - Count How many employees were hired between Jan 1 1999 and Jan 1 2001
  spark.sql(
    """ select * from employees where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin).show()

  spark.sql(
    """ select count(*) as number_employees from employees where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin).show()


  //2 -show the average salaries for the employees hired in between those dates, grouped by depto number
  spark.sql(
    """
      |select avg(salary) as avg_salaries, dep.dept_no from employees e
      |inner join dept_emp dep ON (e.emp_no  = dep.emp_no)
      |inner join salaries sal ON (dep.emp_no = sal.emp_no)
      |where
      |e.hire_date Between '1999-01-01' and '2001-01-01'
      |group by
      |dep.dept_no
      |""".stripMargin
  ).show()


  //3 - Show the name of the best-paying deparment for employeess hired in between those dates

  spark.sql(
    """
      |select avg(salary) as payments, dep.dept_no, d.dept_name
      |from employees e
      |inner join dept_emp dep ON (e.emp_no  = dep.emp_no)
      |inner join salaries sal ON (dep.emp_no = sal.emp_no)
      |inner join departments d ON(dep.dept_no = d.dept_no)
      |where
      |e.hire_date Between '1999-01-01' and '2001-01-01'
      |group by
      |dep.dept_no,  d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin
  ).show()

}
