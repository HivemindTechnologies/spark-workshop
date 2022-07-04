package chapter1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, count, countDistinct, min}

object SparkShellExamples extends App {

  val spark = SparkSession.builder()
    .appName("SparkShellApp")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  /**
   * Queies sample ran from spark-shell
   */

  val amazonReviewsDataframe = spark
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/amazon-reviews-sample.json")

  amazonReviewsDataframe.show(4)
  amazonReviewsDataframe.printSchema()

  // counting all
  amazonReviewsDataframe.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting distinct
  amazonReviewsDataframe.select(countDistinct(col("reviewerID"))).show()

  // approximate count
  amazonReviewsDataframe.select(approx_count_distinct(col("reviewerID")))

  // min and max
  val minRatingDF = amazonReviewsDataframe.select(min(col("overall")))
  amazonReviewsDataframe.selectExpr("min(overall)").show()
  amazonReviewsDataframe.selectExpr("max(overall)").show()

  // Grouping
  val countByReviewer = amazonReviewsDataframe
    .groupBy(col("reviewerName")) // includes null
    .count()  // select count(*) from amazonReviewsDataframe group by reviewerName

  val avgRatingByReviewerDF = amazonReviewsDataframe
    .groupBy(col("reviewerID"))
    .avg("overall")

  avgRatingByReviewerDF.show(4)
}
