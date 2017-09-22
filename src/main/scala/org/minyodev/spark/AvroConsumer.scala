package org.minyodev.spark

import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.minyo.schema.avro.TestAvro

/**
  * This program consume records from Kafka in AVRO format and print messages
  * */
object AvroConsumer {

  val conf = ConfigFactory.load("examples")
  val brokers = conf.getString("kafka.brokers")
  val topics = conf.getString("kafka.topics.avro.consumer.sources")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)

    if(conf.getBoolean("is.local.mode") == true)
      sparkConf.setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.Predef.Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    lines.mapPartitions(itr => {
      itr.map(bytes => {
        val reader = new SpecificDatumReader[TestAvro](TestAvro.SCHEMA$)
        val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
        val test = reader.read(null, decoder)

        "Name: " + test.getName + ", Age: " + test.getAge + ", Gender: " + test.getGender
      })
    })
      .print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
