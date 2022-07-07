package chapter1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameExercises {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("DataframeExercises")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  /**
   * Exercise
   * 1. Make a dataset of the amazon reviews with only reviewerId, overall and asin fields
   * 2. Rename the overall field as ratings
   * 2. Count how many ratings are over 3.0
   * 3. Calculate the average of the ratings column for all the data
   * 4. Write a reader function reading tsv files
   */


  /**
   * Exercise
   * 1. Join all the IMDb datasets together except akasDf and episodeDf
   * 2. From our data samples how many directors got an average ratings superior to 8.0?
   */

}
