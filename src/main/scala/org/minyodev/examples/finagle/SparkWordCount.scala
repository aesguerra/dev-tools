package org.minyodev.examples.finagle

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @author aesguerra
  * */
object SparkWordCount {

  val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName)
  val sc = new SparkContext(conf)

  def processWordCount(): Array[(String, Int)] = {
    val l = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
      "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, " +
      "when an unknown printer took a galley of type and scrambled it to make a type specimen book. " +
      "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. " +
      "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, " +
      "and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."
    val rdd = sc.parallelize(l.split("")).map(w => (w, 1)).reduceByKey(_ + _)
    rdd.collect
  }

  def stop() = sc.stop()
}