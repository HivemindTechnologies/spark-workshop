name := "spark-workshop"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"
val postgresVersion = "42.2.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  "org.postgresql" % "postgresql" % postgresVersion
)