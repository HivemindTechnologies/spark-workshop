# Spark Workshop
Spark is the most unified computing engine and libraries for distributed data processing at scale.
It supports a variety of data processing tasks:
- data loading
- SQL queries
- machine learning
- streaming
- graph processing

Spark is smart enough to optimize your code and the execution of that code on the cluster across different libraries.
As a computing engine it is detached from data storage and I/O.
*"When computing a result, the same execution engine is used, independent of which API/language you are using to express the computation. This unification means that developers can easily switch back and forth between different APIs based on which provides the most natural way to express a given transformation."*[Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)

This repository contains the training material for the Spark Workshop. The workshop makes use of 2 main Spark programming APIs: 
- [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) (Chapter 1 and 2)
- [Spark MLlib](https://spark.apache.org/docs/latest/ml-guide.html) (Chapter 3)  
  
Spark runs on Java 8/11, Scala 2.12/2.13, Python 3.6+ and R 3.5+. Python 3.6 support is deprecated as of Spark 3.2.0. Java 8 prior to version 8u201 support is deprecated as of Spark 3.2.0. For the Scala API, Spark 3.2.1 uses Scala 2.12 or 2.13.   

## Prerequesites
- Spark 3.2.1
- Java 8 or 11
- Docker
- SBT 1.6.2
- Scala 2.13.8

## Spark installation
1. [Download Spark 3.2.1](https://spark.apache.org/downloads.html) following the official website instructions. From command line:
```shell
$ wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
```
2. Decompress the archive:
```shell
$ tar -xvzf spark-3.2.1-bin-hadoop3.2.tgz
```
3. Move the extracted Spark directory to /opt directory:
```shell
$ sudo mv spark-3.2.1-bin-hadoop3.2 /opt/spark
```
4. Configure environmental variables for Spark, according to your `.profile` file (e.g. .bashrc, .zshrc)
```shell
$ echo "export SPARK_HOME=/opt/spark" >> ~/.profile
$ echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.profile
```
4. Source your `.profile`:
```shell
$ source ~/.profile
```
You should have successfully installed Spark on your machine. To verify, try and launch the spark shell:
```shell
$ spark-shell
```

With "Big Data" you have volumes of data that cannot fit on a standard computer. So you need a cluster of computers that can process this data. Spark was built specifically for that task.
For the purpose of this workshop, we will be running Spark in a standalone mode. For some of the examples we will load JSON, csv and parquet formatted data in small samples. You can download the larger examples files here :
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
Base Spark App running in a dedicated container.

## Spark SQL API
Spark SQL offers a framework for loading, querying and persisting structured and semi-structured data using structured queries that can equally be expressed using:
- SQL (with subqueries)
- Custom high-level SQL-like, declarative, type-safe Dataset API (for a structured query DSL)
- Hive QL

### Chapter 1: DataFrames (and Datasets) API
Data loading, analysis and transformation from different data sources format using Spark SQL library.
*"A Dataset is a distributed collection of data. Dataset is a new interface added in Spark 1.6 that provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine. A Dataset can be constructed from JVM objects and then manipulated using functional transformations (map, flatMap, filter, etc.)."*[Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)

### Chapter 2: Using Spark SQL with sql statements
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
**From external sources (S3, Hive,...)**
Spark is in charge of the metadata only. If you drop the Spark SQL table, you keep the data.

#### spark-sql
Interaction from the spark-sql shell.

## Spark MlLib API
Applying Machine Learning preprocessing and building prediction models using our dataframes.
