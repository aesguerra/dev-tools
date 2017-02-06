package org.minyodev

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by aesguerra on 2/1/17.
  */
object Playground extends App {
  val sconf = new SparkConf()
  sconf.setAppName("playground").setMaster("local[*]")
  val sc = new SparkContext(sconf)

  println(">> " + System.getProperty("hello"))

}
