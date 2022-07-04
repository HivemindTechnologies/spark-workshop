package chapter2

import org.apache.spark.sql.SparkSession

object SqlExamples extends App {

  val spark = SparkSession.builder()
    .appName("SQLApp")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  val surfBreaksDataframe = spark
    .read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/coastal_plan_surf_breaks.csv")


  surfBreaksDataframe.show(4)
  surfBreaksDataframe.printSchema()
  surfBreaksDataframe.createOrReplaceTempView("surfCoast")

  // use Spark SQL
  val beachSpotDataframe = spark.sql(
    """
      |select * from surfCoast where Type != 'Beach'
    """.stripMargin)

  beachSpotDataframe.show(4)

  //TODO:
  // - Load and run SQL prepared statements from external file
  // - Add Loading/saving from S3 bucket
  // - Load from PostgreSQL test data

}
