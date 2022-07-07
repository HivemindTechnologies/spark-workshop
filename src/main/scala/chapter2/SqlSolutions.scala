package chapter2

import chapter2.SqlFileHelper.fromTSVToDf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SqlSolutions extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  implicit val spark = SparkSession.builder()
    .appName("SQLSolutions")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  /**
   * Exercises:
   * Using spark.sql:
   * 1. Count the number of cinematographer from the principals
   * 2. Calculate the total number of votes from the ratings
   */

  val ratingsDf = fromTSVToDf("title-ratings-sample")
  val principalsDf = fromTSVToDf("title-principals-sample")

  principalsDf.createOrReplaceTempView("principals")
  spark.sql(
    """
      |select count(*)
      |from principals
      |where category = 'cinematographer'
      |""".stripMargin
  ).show()

  ratingsDf.createOrReplaceTempView("ratings")
  spark.sql(
  """
  |select sum(numVotes) as sumVotes
  |from ratings
  """.stripMargin).show()

}
