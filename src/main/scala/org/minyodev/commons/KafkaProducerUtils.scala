package org.minyodev.commons

import java.util.HashMap

import org.apache.kafka.clients.producer.ProducerConfig

object KafkaProducerUtils {

  /**
    * Zookeeper connection properties ByteArray messages (we use this for producing AVRO format messages)
    * */
  def byteArrayProperty(brokers: String): HashMap[String, Object] = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }

  /**
    * Zookeeper connection properties for string messages
    * */
  def stringProperty(brokers: String): HashMap[String, Object] = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}
