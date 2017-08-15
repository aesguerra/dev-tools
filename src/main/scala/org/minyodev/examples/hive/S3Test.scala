package org.minyodev.examples.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by aesguerra on 6/2/17.
  */
object S3Test extends App {
  val conf = new SparkConf()
  conf.setAppName("SparkSQL2Hive")
  conf.setMaster("local[*]")

  val sc = new SparkContext(conf)
  val spark = SparkSession.builder()
    .master("local")
    .appName("Word Count")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
//    .config("spark.sql.warehouse.dir", warehouseLocation)
//    .config(" hive.metastore.warehouse.dir", "/user/hive/warehouse")
    .getOrCreate()

  val sqlContext = spark.sqlContext

  val hiveContext = new HiveContext(sc)


  val a = spark.read.parquet("file:///home/aesguerra/s3proxy/parquet-minerva/2017052415/20170524160157/part-r-00000.parquet")
  val table = a.toDF().createOrReplaceTempView("mytable")

  spark.sql("select * from mytable limit 50").collect().foreach(println)
  println("------------------------------")
  hiveContext.sql("SHOW DATABASES;").collect().foreach(println)

  HiveThriftServer2.startWithContext(sqlContext)
}
