package chapter1

import chapter1.DataFrameSolutions.fromTSVToDf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{asc, avg, col, count, countDistinct, desc, mean, min, regexp_extract, stddev, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameExamples extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  /**
   * Spark Configuration
   */

  // Entry point to programming Spark with the Dataset and DataFrame API
  val spark = SparkSession.builder()
    .appName("DataframesApp")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  /**
   * Dataframes section
   */
  // Load from different kind of data sources
  val surfBreaksDataframe: DataFrame = spark
    .read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/coastal_plan_surf_breaks.csv")

  val amazonReviewsDataframe: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/amazon-reviews-sample.json")

  // Visualize data sample
  surfBreaksDataframe.show(4)
  amazonReviewsDataframe.show(4)

  //surfBreaksDataframe.printSchema()
  amazonReviewsDataframe.printSchema()

  // spark types
  val longType = LongType

  // schema
  val sparkSchema: StructType = surfBreaksDataframe.schema

  //println(s"Spark inferred schema for surfBreaksDataframe:\n$sparkSchema")

  val surfBreaksSchema = StructType(Array(
    StructField("X", DoubleType),
    StructField("Y", DoubleType),
    StructField("OBJECTID", IntegerType),
    StructField("Name", StringType),
    StructField("Type", StringType),
    StructField("Scale", IntegerType)
  ))

  val surfBreaksDataframeWithSchema: DataFrame = spark
    .read
    .schema(surfBreaksSchema)
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/coastal_plan_surf_breaks.csv")

  surfBreaksDataframeWithSchema.printSchema()

  // counting all
  amazonReviewsDataframe.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting distinct
  amazonReviewsDataframe.select(countDistinct(col("overall")))

  // min and max
  amazonReviewsDataframe.select(min(col("overall")))
  amazonReviewsDataframe.selectExpr("min(overall)")

  // sum
  amazonReviewsDataframe.select(sum(col("overall")))

  // avg
  amazonReviewsDataframe.select(avg(col("overall")))

  // data science
  amazonReviewsDataframe.select(
    mean(col("overall")),
    stddev(col("overall"))
  )

  // group by
  val aggregationsByOverallCountDF = amazonReviewsDataframe
    .groupBy(col("overall"))
    .agg(
      count("*").as("cnt"),
    )
    .orderBy(asc("overall"))

  aggregationsByOverallCountDF.show()

  /**
   * Datasets section
   */
  case class SurfCoast(X: Double,
                       Y: Double,
                       OBJECTID: String,
                       Name: String,
                       Type: String,
                       Scale: Option[Int])

  import spark.implicits._
  val surfDataset = surfBreaksDataframeWithSchema.as[SurfCoast]

  // Dataset collection functions: map, flatMap, fold, reduce, for comprehensions ...
  surfDataset.map(row => row.X * 2)
  surfDataset.filter(_.Type == "Reef")


  // You can also use the dataframe functions!
  surfDataset.select(avg(col("X")))

  // Fill Null values!
  surfBreaksDataframe.select("*").where(col("Scale").isNull)
  surfBreaksDataframe.na.fill(0, List("Scale"))
  surfBreaksDataframe.select("Scale").na.drop() // remove rows containing nulls

  // Further than Dataset[T]:
  // Doric github https://github.com/hablapps/doric
  // Doric Scala Love talk: https://www.youtube.com/watch?v=mJCNIV_9plQ&list=PLBqWQH1MiwBTMk9HV-RNN7sQpB9ZPi_Az&index=17

  /**
   * Exercises
   * 1. Make a dataset of the amazon reviews with only revieweId, overall and asin fields
   * 2. Rename the overall field as ratings
   * 2. Count how many ratings are over 3.0
   * 3. Calculate the average of the ratings column for all the data
   * 4. Write a reader function reading tsv files
   */

  // Load from tsv file
  val ratingsDf = fromTSVToDf("title-ratings-sample")
  val principalsDf = fromTSVToDf("title-principals-sample")
  val crewDf = fromTSVToDf("title-crew-sample")
  val titleBasicsDf = fromTSVToDf("title-basics-sample")
  val namesDf = fromTSVToDf("name-basics-sample")
  val akasDf = fromTSVToDf("title-akas-sample")
  val episodeDf = fromTSVToDf("title-episode-sample")

    val principalsDf2 = principalsDf.selectExpr(
      "tconst",
      "category"
    )

    // column renamed with select
    val principalsDf3 = principalsDf2
      .select("tconst", "category")
      .withColumn("titlesId", col("tconst"))

    val atLeastEight = ratingsDf.select("tconst", "averageRating")
      .where(col("averageRating") > 8 and col("numVotes") > 50)


    // contains
    namesDf
      .select("*")
      .where(col("primaryName").contains("Fred"))
      .drop("deathYear")

    // regex
    val regexString = "actor|actress"
    val actorsDataFrame = namesDf.select(
      col("*"),
      regexp_extract(col("primaryProfession"), regexString, 0).as("regex_extract")
    ).where(col("regex_extract") =!= "").drop("regex_extract")

    // Joins
    // inner joins
    val joinCondition = namesDf.col("nconst") === principalsDf.col("nconst")
    val principalsWithNamesDataframe = namesDf.join(principalsDf, joinCondition, "inner")
    principalsWithNamesDataframe.show(4)

    val ratingJoinCondition = principalsWithNamesDataframe.col("tconst") === ratingsDf.col("tconst")
    val finalDataframe = principalsWithNamesDataframe
      .join(ratingsDf, ratingJoinCondition, "inner")

  /**
   * Exercise
   * 1. Join all the IMDb datasets together except akasDf and episodeDf
   * 2. From our data samples how many directors got an average ratings superior to 8.0?
   */

  //Thread.sleep(30000)
}
