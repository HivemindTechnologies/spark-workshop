package chapter1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameSolutions extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("DataframeSolutions")
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

  val amazonReviewsDataframe: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/amazon-reviews-sample.json")

  case class Reviews(asin: String, overall: Double, reviewerID: String)

  import spark.implicits._
  val reviews = amazonReviewsDataframe.as[Reviews]
  val reviews2 = reviews.withColumnRenamed("overall", "ratings")
  reviews2.select("ratings").where(col("ratings") > 3.0).count()
  reviews2.select(avg(col("ratings")))

  def fromTSVToDf(filename: String): DataFrame = spark
    .read
    .option("header", "true")
    .option("sep", "\t")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv(s"src/main/resources/data/$filename.tsv")


  /**
   * Exercise
   * 1. Join all the IMDb datasets together except akasDf and episodeDf
   * 2. From our data samples how many directors got an average ratings superior to 8.0?
   */

  val ratingsDf = fromTSVToDf("title-ratings-sample")
  val principalsDf = fromTSVToDf("title-principals-sample")
  val crewDf = fromTSVToDf("title-crew-sample")
  val titleBasicsDf = fromTSVToDf("title-basics-sample")
  val namesDf = fromTSVToDf("name-basics-sample")
  val akasDf = fromTSVToDf("title-akas-sample")
  val episodeDf = fromTSVToDf("title-episode-sample")

  // Load and visualize the IMDb datasets
  //namesDf.show(4)
  //principalsDf.show(4)
  //ratingsDf.show(4)
  //crewDf.show(4)
  //titleBasicsDf.show(4)
  //akasDf.show(4)
  //episodeDf.show(4)

  namesDf
    .join(principalsDf).where(namesDf("nconst") === principalsDf("nconst"))
    .join(ratingsDf).where(principalsDf("tconst") === ratingsDf("tconst"))
    .join(crewDf).where(ratingsDf("tconst") === crewDf("tconst"))
    .join(titleBasicsDf).filter(titleBasicsDf("tconst") === crewDf("tconst"))

  namesDf
    .join(principalsDf, principalsDf.col("nconst") === namesDf.col("nconst"), "outer")
    .join(ratingsDf, principalsDf.col("tconst") === ratingsDf.col("tconst"), "outer")
    .where(col("category") === "director" and col("averageRating") > 8.0)

  principalsDf
    .join(ratingsDf).where(principalsDf("tconst") === ratingsDf("tconst"))
    .where(col("category") === "director" and col("averageRating") > 8.0)

}
