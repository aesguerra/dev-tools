package org.minyodev.examples.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aesguerra on 6/1/17.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkSQL2Hive")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS spark")
    spark.sql("use spark")
    // stu_age
    spark.sql("DROP TABLE IF EXISTS stu_age") // table
    spark.sql("CREATE TABLE IF NOT EXISTS stu_age(name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    //hive: LOAD DATA INPATH HDFS Hive
    spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/stu_age.txt' INTO TABLE stu_age")
    // stu_score
    spark.sql("DROP TABLE IF EXISTS stu_score")
    spark.sql("CREATE TABLE IF NOT EXISTS stu_score(name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/stu_score.txt' INTO TABLE stu_score")

    //join
    val stu = "SELECT a.name,a.age,s.score FROM spark.stu_age a JOIN spark.stu_score s ON a.name=s.name where s.score > 90"

    spark.sql("DROP TABLE IF EXISTS students") // Table
    spark.sql("CREATE TABLE IF NOT EXISTS students(name STRING,age INT,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")

    spark.sql("insert into table students " + stu)

    // HivewContext Table Hive Table DaraFrame
    // ETL
    val dataFromHive = spark.table("students")
    dataFromHive.show()

    spark.sql("show SCHEMAS").collect().foreach(println)
  }
}