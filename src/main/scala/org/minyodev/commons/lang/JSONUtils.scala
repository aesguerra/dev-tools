package org.minyodev.commons.lang

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * JsonUtils provides functionality for reading JSON records, as well as
  * related functionality for performing conversions.
  * <p>
  * Sample usage can be found at {@link com.cloud4wi.examples.EventMessages}
  * hello world
  * */
object JSONUtils {

  type ObjectMapperUtil = ObjectMapper with ScalaObjectMapper

  /**
    * This method will basically create an object mapper that we will be using to read event messages from Kafka
    * */
  def createObjectMapper(): ObjectMapperUtil = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  def toJson(mapper: ObjectMapperUtil, value: Any): String = mapper.writeValueAsString(value)
  def fromJson[T](mapper: ObjectMapperUtil, json: String)(implicit m : Manifest[T]): T = mapper.readValue[T](json)
  def fromJsonToMap[V](mapper: ObjectMapperUtil, json: String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](mapper, json)

  /**
    * JSON strings coming from Debezium without sanity checks
    * */
  def toMap(mapper: ObjectMapperUtil, json: String): Map[String, Any] = {

    // TODO Change me if already done with Debezium json payload fix
    JSONUtils.fromJsonToMap[Any](mapper, json)
      .get("payload").get
      .asInstanceOf[Map[String, Any]]
      .get("after").get
      .asInstanceOf[Map[String, Any]]

    //  JSONUtils.fromJsonToMap[Any](mapper, json)
    //    .get("after")
    //    .get.asInstanceOf[Map[String, Any]]
  }

  /**
    * Sanity checks for JSON strings coming from Debezium
    * */
  def toMapWithSanityChecks(mapper: ObjectMapperUtil, json: String): Map[String, Any] = {

    // The message value can be null for Kafka version < 0.10
    // However as of Kafka 0.10, the JSON converter provided by Kafka Connect
    // never results in a null value for the message (KAFKA-3832)
    // More info here: http://debezium.io/docs/connectors/mysql/
    if (json == null || json == "null") return null

    // We only get 'after' field
    // 'after' is an optional field that if present contains the state of the row after the event occurred.
    // The structure is describe by the same mysql-server-1.inventory.customers.Value Kafka Connect schema used in before.
    // More info here: http://debezium.io/docs/connectors/mysql/
    //  val after = JSONUtils.fromJsonToMap[Any](mapper, json).get("after") match {
    //    case Some(a) => a
    //    case None => null
    //  }

    // TODO Change me if already done with Debezium json payload fix
    val after = JSONUtils.fromJsonToMap[Any](mapper, json).get("payload") match {
      case Some(a) => a.asInstanceOf[Map[String, Any]].get("after") match {
        case Some(b) => b
        case None => null
      }
      case None => null
    }

    // Sanity checks
    if(after == null) return null

    val map = after.asInstanceOf[Map[String, Any]]

    // There can be an instance where 'after field' exists but it's empty, thus, return false
    if(map.isEmpty) return null

    return map
  }
}
