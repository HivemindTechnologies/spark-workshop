package chapter2

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.util.Using

object SqlFileHelper {

  def getSqlFromResourceFile(resourceName: String)(implicit spark: SparkSession): String = {

    val sqlText = Using(scala.io.Source.fromResource(resourceName)) { bufferedSource =>
      bufferedSource.getLines.mkString("\n")
    }.get //no way to recover --> let it crash
    sqlText
  }


  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/spark-workshop"
  val user = "hivemind"
  val password = "hivemind-password"

  val getJdbcWriteConnectionProperties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", "org.postgresql.Driver")
    connectionProperties
  }


  def fromTSVToDf(filename: String)(implicit spark: SparkSession): DataFrame =
    spark
    .read
    .option("header", "true")
    .option("sep", "\t")
    .option("multiLine", "true")
    .option("quote","\"")
    .option("escape","\"")
    .option("ignoreTrailingWhiteSpace", true)
    .csv(s"src/main/resources/data/$filename.tsv")
}
