package chapter3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object MLlibExamples extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("MLlibApp")
    .config("spark.master", "local")
    .getOrCreate()

  // Non-verbose mode, comment to add INFO logs
  spark.sparkContext.setLogLevel("WARN")

  val airbnbSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("price", DoubleType),
    StructField("bedrooms", DoubleType),
    StructField("bathrooms", DoubleType),
    StructField("room_type", StringType),
    StructField("square_feet", DoubleType),
    StructField("host_is_superhost", DoubleType),
    StructField("state", StringType),
    StructField("cancellation_policy", StringType),
    StructField("security_deposit", DoubleType),
    StructField("cleaning_fee", DoubleType),
    StructField("extra_people", DoubleType),
    StructField("number_of_reviews", DoubleType),
    StructField("price_per_bedroom", DoubleType),
    StructField("review_scores_rating", DoubleType),
    StructField("instant_bookable", IntegerType)
  ))

  val airbnbDf: DataFrame = spark
    .read
    .schema(airbnbSchema)
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv("src/main/resources/data/airbnb.csv")
    .na.fill(0)

  //airbnbDf.printSchema()

  // Data cleaning and analysis
  airbnbDf.createOrReplaceTempView("airbnb")
  val airbnbStandardized = spark.sql("""
      select
          id,
          case when state in('NY', 'CA', 'London', 'Berlin', 'TX' ,'IL', 'OR', 'DC', 'WA')
              then state
              else 'Other'
          end as state,
          price,
          bathrooms,
          bedrooms,
          room_type,
          host_is_superhost,
          cancellation_policy,
          case when security_deposit is null
              then 0.0
              else security_deposit
          end as security_deposit,
          price_per_bedroom,
          case when number_of_reviews is null
              then 0.0
              else number_of_reviews
          end as number_of_reviews,
          case when extra_people is null
              then 0.0
              else extra_people
          end as extra_people,
          instant_bookable,
          case when cleaning_fee is null
              then 0.0
              else cleaning_fee
          end as cleaning_fee,
          case when review_scores_rating is null
              then 80.0
              else review_scores_rating
          end as review_scores_rating,
          case when square_feet is not null and square_feet > 100
              then square_feet
              when (square_feet is null or square_feet <=100) and (bedrooms is null or bedrooms = 0)
              then 350.0
              else 380 * bedrooms
          end as square_feet,
          case when bathrooms >= 2
              then 1.0
              else 0.0
          end as n_bathrooms_more_than_two
      from airbnb
      where bedrooms is not null
      """)
  airbnbStandardized

  spark.sql("""
    select
        state,
        count(*) as n,
        cast(avg(price) as decimal(12,2)) as avg_price,
        max(price) as max_price
    from airbnb
    group by state
    order by avg(price) desc
  """).filter("n>25")

  // Define continuous and categorical features
  val continuousFeatures = List("bathrooms", "bedrooms", "security_deposit", "cleaning_fee", "extra_people", "number_of_reviews", "square_feet", "review_scores_rating")
  val categoricalFeatures = List("room_type", "host_is_superhost", "cancellation_policy", "state")
  val features = continuousFeatures ++ categoricalFeatures

  // Split data into training and test samples
  val Array(train, test) = airbnbStandardized.randomSplit(Array(0.8, 0.2), seed = 1234)

  // Categorical features pipeline
  val indexer: StringIndexer =
      new StringIndexer()
        .setInputCols(categoricalFeatures.toArray)
        .setOutputCols(categoricalFeatures.map(name => s"${name}_indexed").toArray)
        .setHandleInvalid("keep")

  val oneHotEncoder: OneHotEncoder =
     new OneHotEncoder()
      .setInputCols(categoricalFeatures.map(name => s"${name}_indexed").toArray)
      .setOutputCols(categoricalFeatures.map(name => s"${name}_onehot").toArray)

  val categoricalPipeline: Pipeline = new Pipeline("categorical").setStages(Array(indexer, oneHotEncoder))
  val model: PipelineModel = categoricalPipeline.fit(train)
  val transformedDf = model.transform(train).show(4)


  // Continuous features pipeline
  val vectorAssemblerContinuousFeatures: VectorAssembler =
    new VectorAssembler()
      .setInputCols(continuousFeatures.toArray)
      .setOutputCol("unscaled")
      .setHandleInvalid("keep")

  val standardScaler: StandardScaler =
    new StandardScaler()
      .setInputCol("unscaled")
      .setOutputCol("scaled")

  val vectorAssemblerAllFeatures: VectorAssembler =
    new VectorAssembler()
      .setInputCols("scaled" +: categoricalFeatures.map(name => s"${name}_onehot").toArray)
      .setOutputCol("features")
      .setHandleInvalid("keep")

  val fullPipeline = new Pipeline("full").setStages(Array(categoricalPipeline, vectorAssemblerContinuousFeatures, standardScaler))
  fullPipeline.fit(train).transform(train).select("scaled", "price")


  val linearRegression: LinearRegression = new LinearRegression()
    .setFeaturesCol("scaled")
    .setLabelCol("price")
    .setPredictionCol("price_prediction")
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val linearRegressionPipeline = new Pipeline("linear regression").setStages(Array(fullPipeline, linearRegression))
  val lrModel: PipelineModel = linearRegressionPipeline.fit(train)
  lrModel.transform(test).select("price", "price_prediction")


  val logisticRegression: LogisticRegression = new LogisticRegression()
    .setFeaturesCol("scaled")
    .setLabelCol("n_bathrooms_more_than_two")
    .setPredictionCol("n_bathrooms_more_than_two_prediction")
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val logisticRegressionPipeline = new Pipeline("logistic regression").setStages(Array(fullPipeline, logisticRegression))
  val logRegModel: PipelineModel = logisticRegressionPipeline.fit(train)
  val pred = logRegModel.transform(test).select("n_bathrooms_more_than_two", "n_bathrooms_more_than_two_prediction")
}
