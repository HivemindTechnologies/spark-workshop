package chapter2

import chapter2.SqlFileHelper.{driver, fromTSVToDf, getJdbcWriteConnectionProperties, password, url, user}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SqlExamples extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  implicit val spark = SparkSession.builder()
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

  surfBreaksDataframe.createOrReplaceTempView("surfcoast")

  // use Spark SQL
  val beachSpotDataframe = spark.sql(
    """
      |select * from surfcoast
      |where Type != 'Beach'
    """.stripMargin)

  /**
   * Exercises:
   * Using spark.sql:
   * 1. Count the number of cinematographer from the principals
   * 2. Calculate the total number of votes from the ratings
   */

  val firstSqlQuery    = SqlFileHelper.getSqlFromResourceFile(s"spark-sql/firstQuery.sql")
  val reefBreaksDataframe: DataFrame = spark.sql(firstSqlQuery)

  // write to an external database
  reefBreaksDataframe.write.mode("append").jdbc(url, "reefbreaks", getJdbcWriteConnectionProperties)

  // transfer tables from a DB to Spark tables
  def readTable(tableName: String) =
    spark
      .read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

  def transferfromDb(tableNames: List[String]) =
    tableNames.foreach { tableName =>
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
    }
}
