package org.minyodev.spark

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.minyodev.commons.KafkaProducerUtils

object StringProducer {

  val conf = ConfigFactory.load("examples")
  val brokers = conf.getString("kafka.brokers")
  val topic = conf.getString("kafka.topics.string.producer.sink")
  val messagesPerSec = "20"
  val wordsPerMessage = "2"

  def main(args: Array[String]) {

    // Zookeeper connection properties
    val props = KafkaProducerUtils.stringProperty(brokers)
    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt)
          .map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        println("Sending message: " + str)
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}
