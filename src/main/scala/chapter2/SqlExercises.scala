package chapter2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SqlExercises extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  implicit val spark = SparkSession.builder()
    .appName("SQLExercises")
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
}
