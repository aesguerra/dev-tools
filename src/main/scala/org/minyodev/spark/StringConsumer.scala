package org.minyodev.spark

import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * A Program that will consume records from Kafka, perform Wordcount and simply print out output
  *
  * This program is readily available at
  * {@link https://github.com/apache/spark/tree/v2.1.1/examples/src/main/scala/org/apache/spark/examples/streaming}
  *
  */
object StringConsumer {

  val conf = ConfigFactory.load("examples")
  val brokers = conf.getString("kafka.brokers")
  val topics = conf.getString("kafka.topics.string.consumer.sources")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)

    if(conf.getBoolean("is.local.mode") == true)
      sparkConf.setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.Predef.Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
