package org.minyodev.examples.phoenix

import java.sql.{Connection, DriverManager}

/**
  * Created by aesguerra on 2/6/17.
  */
object SimpleQuery extends App {
  var con: Connection = _
  try {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    // jdbc:phoenix:<ZK-QUORUM>:<ZK-PORT>:<ZK-HBASE-NODE>
    con = DriverManager.getConnection("jdbc:phoenix:localhost:2181")
    val st = con.createStatement()
    val rs = st.executeQuery("select * from MINERVA")

    while (rs.next) {
      val tenantId = rs.getString("TENANT_ID")
      val venueId = rs.getString("VENUE_ID")
      val apId = rs.getString("ACCESS_POINT_ID")
      println("Tenant ID: " + tenantId + ", Venue ID: " + venueId + ", Access Point ID: " + apId)
    }

  } catch {
    case e: Exception => e.printStackTrace
  }
  con.close
}
