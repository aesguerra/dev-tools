package org.minyodev.spark

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Produces some random words between 1 and 100 to Kafka.
  *
  * This program is readily available at https://github.com/apache/spark/tree/v2.1.1/examples/src/main/scala/org/apache/spark/examples/streaming
  *
  * @author aesguerra
  */
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) =
      Array("localhost:9092", "sample-topic", "20", "2")

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt)
          .map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}
