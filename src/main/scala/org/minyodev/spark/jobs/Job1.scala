package org.minyodev.spark.jobs

import org.minyodev.commons.SparkJobDriver.SparkJob
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by aesguerra on 1/18/17.
  */
object Job1 extends SparkJob {
  override val name = "Job 1"

  override def run(sc: SparkContext, args: Array[String]): Unit = {
    val s = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
    val arr = s.split("")
    val rdd = sc.parallelize(arr)
    val fmt = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    rdd.map(m => (m, 1)).reduceByKey(_+_).saveAsTextFile("/home/aesguerra/sample/job1/" + fmt.print(new DateTime))
  }
}