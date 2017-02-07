package org.minyodev.finagle

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{ParamMap, Request, Response, Status}
import com.twitter.util.{Await, Future}

import scala.collection.mutable.ListBuffer
import scala.sys.Prop

/**
  * Created by aesguerra on 2/6/17.
  */
object PhoenixServer extends App {

  var con: Connection = _
  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
  // jdbc:phoenix:<ZK-QUORUM>:<ZK-PORT>:<ZK-HBASE-NODE>
  con = DriverManager.getConnection("jdbc:phoenix:localhost:2181")
  val st = con.createStatement()

  println(Prop[String]("java.class.path"))
  println(Prop[String]("java.home"))

  val service = new Service[Request, Response] {
    def apply(req: Request): Future[Response] = getTenantVenueAccesspointIDs(req)
  }

  val server = Http.serve(":40000", service)
  Await.ready(server)

  // Get details logged by client
  def getTenantVenueAccesspointIDs = new Service[Request, Response] {

    def checkParams(req: Request): Response = {
      val pm = req.params
      val status =
        if(pm.size > 0)
          // Will not accept if username is empty, sex is empty and age is 0
          if(pm.getOrElse("TENANT_ID", "").equals("")
            || pm.getOrElse("VENUE_ID", "").equals("")
            || pm.getOrElse("ACCESS_POINT_ID", "").equals(""))
            Status.BadRequest
          else
            Status.Ok
        else
          Status.Ok

      Response(req.version, status)
    }

    def getParams(pm: ParamMap): (Long, Long, Long) =
      (
        pm.getOrElse("TENANT_ID", "").toLong,
        pm.getOrElse("VENUE_ID", "").toLong,
        pm.getOrElse("ACCESS_POINT_ID", "").toLong)

    /** entry point of get */

    def apply(req: Request): Future[Response] = {

      val resp = Response(req.version, Status.Ok)

      req.path match {
        case "/user" => Future.value({
          val resp = checkParams(req)

          resp.contentString =
            if (req.params.size > 0) {
              val (tenant: Long, venue: Long, ap: Long) = getParams(req.params)
              val rs = st.executeQuery("select * from MINERVA where TENANT_ID=" + tenant + " AND VENUE_ID=" + venue + " AND ACCESS_POINT_ID=" + ap)
              val list = new ListBuffer[String]
              while (rs.next) {
                val tenantId = rs.getString("TENANT_ID")
                val venueId = rs.getString("VENUE_ID")
                val apId = rs.getString("ACCESS_POINT_ID")
                val sessionsHllCount = rs.getString("SESSIONS_HLL_C")

                list += "Tenant ID: " + tenantId + ", Venue ID: " + venueId + ", Access Point ID: " + apId + ", SESSIONS_HLL_C: " + sessionsHllCount
              }
              list.mkString("\n")
            }
            else
              "Sarreh you did not specify any..."

          //resp.contentType = "application/json"
          resp
        })

        case _ => Future.value(resp)
      }



    }
  }
}
