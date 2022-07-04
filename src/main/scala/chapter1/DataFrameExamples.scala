package chapter1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, regexp_extract, stddev, sum}

object DataFrameExamples extends App {

  val spark = SparkSession.builder()
    .appName("DataframesApp")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  val titleRatingsDataframe = spark
    .read
    .option("header", "true")
    .option("sep", "\t")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/title-ratings-sample.tsv")

  titleRatingsDataframe.show(4)
  titleRatingsDataframe.printSchema()

  // counting all
  titleRatingsDataframe.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting distinct
  titleRatingsDataframe.select(countDistinct(col("tconst"))).show()

  // approximate count
  titleRatingsDataframe.select(approx_count_distinct(col("tconst")))

  // min and max
  val minRatingDF = titleRatingsDataframe.select(min(col("averageRating")))
  titleRatingsDataframe.selectExpr("min(averageRating)")

    // sum
    titleRatingsDataframe.select(sum(col("numVotes")))
    titleRatingsDataframe.selectExpr("sum(numVotes)")

    // avg
    titleRatingsDataframe.select(avg(col("numVotes")))
    titleRatingsDataframe.selectExpr("numVotes")

    // data science
    titleRatingsDataframe.select(
      mean(col("averageRating")),
      stddev(col("averageRating"))
    )

    val aggregationsByNumVotesDF = titleRatingsDataframe
      .groupBy(col("averageRating"))
      .agg(
        count("*").as("ratings"),
        avg("numVotes").as("AverageNumVotesDifferent")
      )
      .orderBy(col("AverageNumVotesDifferent"))

  aggregationsByNumVotesDF.show(4)


  val titlePrincipalsDataframe = spark
    .read
    .option("header", "true")
    .option("sep", "\t")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/title-principals-sample.tsv")

  titlePrincipalsDataframe.show(4)

  val titlePrincipalsDataframe2 = titlePrincipalsDataframe.selectExpr(
    "tconst",
    "category"
  )

  val titlePrincipalsDataframe3 = titlePrincipalsDataframe2
    .select("tconst", "category")
    .withColumn("titlesId", col("tconst"))

  val atLeastEight = titleRatingsDataframe.select("tconst", "averageRating")
    .where(col("averageRating") > 8 and col("numVotes") > 50)

  val onlyDirectors = titlePrincipalsDataframe.select("tconst", "nconst", "category")
    .where(col("category") === "director")

  val namesDataframe = spark
    .read
    .option("header", "true")
    .option("sep", "\t")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/name-basics-sample.tsv")

  namesDataframe.show(4)

  // contains
  namesDataframe.select("*").where(col("primaryName").contains("Fred")).drop("deathYear").show(4)

  // regex
  val regexString = "actor|actress"
  val actorsDataFrame = namesDataframe.select(
    col("*"),
    regexp_extract(col("primaryProfession"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract").show(4)

  // Joins
  // inner joins
  val joinCondition = namesDataframe.col("nconst") === titlePrincipalsDataframe.col("nconst")
  val principalsWithNamesDataframe = namesDataframe.join(titlePrincipalsDataframe, joinCondition, "inner")
  principalsWithNamesDataframe.show(4)

  // outer joins
//  namesDataframe.join(titlePrincipalsDataframe, joinCondition, "left_outer")
//  namesDataframe.join(titlePrincipalsDataframe, joinCondition, "right_outer")
//  namesDataframe.join(titlePrincipalsDataframe, joinCondition, "outer")
//  namesDataframe.join(titlePrincipalsDataframe, joinCondition, "left_semi")

  // Exercise
  val ratingJoinCondition = principalsWithNamesDataframe.col("tconst") === titleRatingsDataframe.col("tconst")
  val finalDataframe = principalsWithNamesDataframe
    .join(titleRatingsDataframe, ratingJoinCondition, "inner")
  finalDataframe.show(4)

  // TODO:
  //  - Add Datasets with case class
  //  - Add na.fill
  // - Save transformed data
}
