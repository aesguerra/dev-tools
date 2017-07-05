package org.minyodev.spark

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * A Program that will consume records from Kafka, perform Wordcount and simply print out output
  *
  * This program is readily available at https://github.com/apache/spark/tree/v2.1.1/examples/src/main/scala/org/apache/spark/examples/streaming
  *
  * @author aesguerra
  */
object DirectKafkaWordCount {

  def main(args: Array[String]): Unit = {

    val Array(brokers, topics) = Array("localhost:9092", "sample-topic")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf()
      .setAppName("DirectKafkaWordCount")
      .setMaster("local[*]")
      .setAppName("Sample")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
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
