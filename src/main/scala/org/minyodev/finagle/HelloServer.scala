package org.minyodev.finagle


import com.twitter.finagle.http.{ParamMap, Request, Response, Status}
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}

import scala.sys.Prop

/**
  * @author aesguerra
  * */
object HelloServer extends App {

  println(Prop[String]("java.class.path"))
  println(Prop[String]("java.home"))

  val service = new Service[Request, Response] {
    def apply(req: Request): Future[Response] = getUserDetailParams(req)
  }

  val server = Http.serve(":40000", service)
  Await.ready(server)

  // Get details logged by client
  def getUserDetailParams = new Service[Request, Response] {

    def checkParams(req: Request): Response = {
      val pm = req.params
      val status = if(pm.size > 0)
        // Will not accept if username is empty, sex is empty and age is 0
        if(pm.getOrElse("username", "").equals("")
          || pm.getOrElse("sex", "").equals("")
          || pm.getOrElse("age", 0).equals(0))
          Status.BadRequest
        else
          Status.Ok
      else
        Status.Ok

      Response(req.version, status)
    }

    def getParams(pm: ParamMap): (String, String, Int) =
      (
        pm.getOrElse("username", "").toString,
        pm.getOrElse("sex", "").toString,
        pm.getOrElse("age", "0").toInt)

    /** entry point of get */

    def apply(req: Request): Future[Response] = {

      val resp = Response(req.version, Status.Ok)

      Future.value({
        val resp = checkParams(req)

        resp.contentString =
          if (req.params.size > 0) {
            val (username: String, sex: String, age: Int) = getParams(req.params)
            "Username: " + username + ", Sex: " + sex + ", Age: " + age
          }
          else
            "Sarreh you did not specify any..."

        resp.contentType = "application/json"
        resp
      })
    }
  }
}

