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

  val people = Seq(
  ("Marie", 30, "Project Manager"),
  ("Bob", 50, "Developer"),
  ("Justine", 34, "Developer")
  )

  val peopleDf = spark.createDataFrame(people)
  import spark.implicits._
  val peopleDf2 = people.toDF("Name", "Age", "Role")

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

}

