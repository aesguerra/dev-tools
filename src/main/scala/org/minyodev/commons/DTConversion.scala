package org.minyodev.commons

import org.joda.time.format.DateTimeFormat

/**
  * <p>Class containing methods to convert ff:</p>
  * <p>-certain unit of time to seconds</p>
  * <p>-datetime string to millis</p>
  * <p>-drop time from epoch date format</p>
  */
class DTConversion[T](x: T) {
  def second = x
  def minute = x.asInstanceOf[Int] * 60
  def hour = x.asInstanceOf[Int] * 60 * 60
  def days = x.asInstanceOf[Int] * 24 * 60 * 60
}

object DTConversion {
  val FMT_DTTM_DASHES = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val FMT_DTHR_DASHES = DateTimeFormat.forPattern("yyyy-MM-dd HH")

  sealed trait toMillis {
    val dt: String

    /**
      * Datetime yyyy-MM-dd HH:mm:ss to millis
      * */
    def fromDttmDashes: java.lang.Long = if(dt == null) null else FMT_DTTM_DASHES.parseDateTime(dt).getMillis

    /**
      * Datehour yyyy-MM-dd HH to millis
      * */
    def fromDthrDashes: java.lang.Long = if(dt == null) null else FMT_DTHR_DASHES.parseDateTime(dt).getMillis

  }

  sealed trait fromMillis {
    val millis: java.lang.Long

    /**
      * Drop minutes time from epoch format date
      * */
    def dropMinutes: Long = (3600000) * (millis/(3600000))

    /**
      * Drop seconds time from epoch format date
      * */
    def dropSeconds: Long = (60000) * (millis/(60000))
  }

  implicit def toMs(d: String) = new toMillis { val dt = d }
  implicit def fromMs(m : java.lang.Long) = new fromMillis { val millis = m }
}