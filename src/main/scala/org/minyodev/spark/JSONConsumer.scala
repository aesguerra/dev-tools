package org.minyodev.spark

import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.minyodev.commons.lang.JSONUtils

object JSONConsumer {

  val conf = ConfigFactory.load("examples")
  val brokers = conf.getString("kafka.brokers")
  val topics = conf.getString("kafka.topics.json.consumer.sources")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)

    if(conf.getBoolean("is.local.mode") == true)
      sparkConf.setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.Predef.Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.map(r => {
      val json = r._2
      val mapper = JSONUtils.createObjectMapper()
      val data = JSONUtils.fromJsonToMap[Any](mapper, json)
        .get("data").get
        .asInstanceOf[Map[String, Any]]

      data
    })
      .print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
