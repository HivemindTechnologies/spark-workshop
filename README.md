# Spark Workshop
This repository contains the training material for the Spark Workshop. The workshop makes use of 2 main Spark programming APIs: 
- [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) (Chapter 1 and 2)
- [Spark MLlib](https://spark.apache.org/docs/latest/ml-guide.html) (Chapter 3)  
  
Each section contains a set of exercises.

Spark runs on Java 8/11, Scala 2.12/2.13, Python 3.6+ and R 3.5+. Python 3.6 support is deprecated as of Spark 3.2.0. Java 8 prior to version 8u201 support is deprecated as of Spark 3.2.0. For the Scala API, Spark 3.2.1 uses Scala 2.12 or 2.13.   

## Prerequesites
- Spark 3.2.1
- Docker
- SBT 1.6.2
- Scala 2.13.8

We will be using JSON, csv and parquet formatted data in small samples. You can download the larger examples files here :
- [amazon-reviews](https://hivemind-share.s3-eu-west-1.amazonaws.com/codingchallenge/resources/amazon-reviews.json.gz)
- [IMDb datasets](https://datasets.imdbws.com/)
- [coastal plan surf breaks](https://catalogue.data.govt.nz/dataset/surf-breaks-bay-of-plenty-proposed-plan-2014)

## Running Spark
You can run Spark interactively or from a client program:
- `spark-shell`: submit interactive statements through the Scala, Python, or R shell, or through a high-level notebook
- **Spark application**: use APIs to create a Spark application that runs interactively or in batch mode, using Scala, Python, R, or Java
In these workshop we will be experimenting with both.   

## Spark Shell
Run the following in the Spark Directory
```sh
$ ./bin/spark-shell
```

## Spark Application
Base Spark App runing in a dedicated container.

## Chapter 1: Spark DataFrame (and Dataset) API
Data loading, analysis and transformation from different data sources format using Spark SQL library.

## Chapter 2: Using Spark SQL with sql statements
Spark SQL is an SQL abstraction over DataFrames to enable engineers familiar with databases to use their knowledge on a Spark cluster. It can be run:
- programmatically in expressions
- from the Spark shell

### How does it work?
This support for SQL in done in 2 ways:
- by importing the concepts of database, table and view
- by allowing engineers to access DataFrames as tables
Dataframes and tables are identical to Spark: storage, partitioning,...etc.
Dataframes can be processed programmatically whereas tables are accessible in SQL.

**Managed Data**
Spark is in charge of the metadata and the data. If you drop the table you lose the data.
**From external sourcesi (S3, Hive,...)**
Spark is in charge of the metadata only. If you drop the Spark SQL table, you keep the data.

We will be following the

### spark-sql
Interaction from the spark-sql shell.

## Spark MlLib
Applying Machine Learning preprocessing and building prediction models using our dataframes.
