package org.minyodev.examples

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.joda.time.format.DateTimeFormat
/**
  * Created by aesguerra on 1/26/17.
  */
object HBaseFileChecker extends App {

  val currentDateHour = DateTimeFormat.forPattern("yyyyMMddHH")

  val c = HBaseConfiguration.create()
  c.set("hbase.zookeeper.quorum", "localhost:2181")

  val connection = ConnectionFactory.createConnection(c)
  val fileStatus = connection.getTable(TableName.valueOf("filestatus"))
  //val rs: ResultScanner = fileStatus.getScanner(Bytes.toBytes("dthr:" + currentDateHour.print(new DateTime)))
  val scan = new Scan


  def getDthrToProcess(): String = {
    val list = scala.collection.mutable.ListBuffer.empty[String]

    val family = Bytes.toBytes("0")
    val qSuccess = Bytes.toBytes("INGEST_S")
    //val qFail = Bytes.toBytes("PP_F")

    val rs = fileStatus.getScanner(family, qSuccess)
    var a = rs.next()

    if(a != null) {
      while(a != null) {
        val value = Bytes.toInt(a.getValue(family, qSuccess))
        if(value == 0) {
          list += (Bytes.toString(a.getRow) + "," + value)
          println(">>> " + Bytes.toString(a.getRow) + "," + value)
          //fileStatus.put(new Put(a.getRow).addColumn(family, qSuccess, Bytes.toBytes("1")))
        }
        a = rs.next()
      }
    }

    println("Date hours to processed: " + list)
    list.mkString(",")
  }

  def incrProcessed(dates: String): Unit = {
    val list = scala.collection.mutable.ListBuffer.empty[String]

    dates.split(",").foreach(dt => {
      val family = Bytes.toBytes("0")

      val qSuccess = Bytes.toBytes("INGEST_S")
      //val qFail = Bytes.toBytes("PP_F")

      // row, family, qualifier
      fileStatus.incrementColumnValue(Bytes.toBytes(dt), family, qSuccess, 1)
    })
  }

  def putProcessed(dates: String): Unit = {
    dates.split(",").map(dt => {
      val put = new Put(dt.getBytes())
      put.addColumn("0".getBytes(), "INGEST_S".getBytes(), Bytes.toBytes(0L))
      fileStatus.put(put)
    })
  }

  incrProcessed("2017012712,2017012713,2017012714")

  //println("datetoprocess: " + getDthrToProcess)

//  val put = new Put("2017012712".getBytes())
//  put.addColumn("0".getBytes(), "INGEST_S".getBytes(), Bytes.toBytes(0L))
//  fileStatus.put(put)
//  val put2 = new Put("2017012713".getBytes())
//  put2.addColumn("0".getBytes(), "INGEST_S".getBytes(), Bytes.toBytes(0L))
//  fileStatus.put(put)
//  val put3 = new Put("2017012714".getBytes())
//  put3.addColumn("0".getBytes(), "INGEST_S".getBytes(), Bytes.toBytes(0L))
//  fileStatus.put(put)
//  fileStatus.put(put2)
//  fileStatus.put(put3)


  println("ok")
}
