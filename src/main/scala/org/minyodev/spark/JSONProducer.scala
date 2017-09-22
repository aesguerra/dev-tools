package org.minyodev.spark

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * This program produce records to Kafka in JSON format
  * */
object JSONProducer {

  val conf = ConfigFactory.load("examples")
  val brokers = conf.getString("kafka.brokers")
  val topic = conf.getString("kafka.topics.json.producer.sink")
  val messagesPerSec = "20"
  val wordsPerMessage = "2"

  def main(args: Array[String]) {

    // Zookeeper connection properties
    val props = KafkaProducerUtils.stringProperty(brokers)
    val producer = new KafkaProducer[String, String](props)

    val random = new Random()
    val gender = List("male", "female", "unknown")

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>

        val mapper = JSONUtils.createObjectMapper()
        val map = Map[String, Any](
          "name" -> ("Foo the " + random.nextInt(100000)),
          "age" -> random.nextInt(70),
          "gender" -> gender(random.nextInt(3))
        )
        val data = Map[String, Any]("data" -> map)
        val str = JSONUtils.toJson(mapper, data)

        println("Sending message: " + str)
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}
