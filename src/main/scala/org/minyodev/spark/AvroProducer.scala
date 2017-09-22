package org.minyodev.spark

import java.io.ByteArrayOutputStream
import java.util.HashMap

import com.typesafe.config.ConfigFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.minyo.schema.avro.TestAvro

import scala.util.Random

/**
  * This program produce records to Kafka in AVRO format
  * */
object AvroProducer {

  val conf = ConfigFactory.load("examples")
  val brokers = conf.getString("kafka.brokers")
  val topic = conf.getString("kafka.topics.json.producer.sink")
  val messagesPerSec = "20"
  val wordsPerMessage = "2"

  def main(args: Array[String]): Unit = {

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[String, Array[Byte]](props)
    val random = new Random()

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt)
          .map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val testAvro = TestAvro.newBuilder()
          .setName("Charlie " + random.nextInt(1000))
          .setAge(39 + random.nextInt(1000))
          .setGender("Male " + random.nextInt(1000))
          .build()

        val out = new ByteArrayOutputStream()

        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        val writer = new SpecificDatumWriter[TestAvro](TestAvro.SCHEMA$)
        writer.write(testAvro, encoder)
        encoder.flush()

        println("Sending message: " + testAvro)
        val message = new ProducerRecord[String, Array[Byte]](topic, null, out.toByteArray())
        producer.send(message)
        out.close()
      }

      Thread.sleep(1000)
    }
  }
}
