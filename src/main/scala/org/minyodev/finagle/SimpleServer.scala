package org.minyodev.finagle


import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}

import scala.sys.Prop
import scala.sys.process.Process

object SimpleServer extends App {

  println(Prop[String]("java.class.path"))
  println(Prop[String]("java.home"))

  def process(resp: Response): Response = {
    resp.setContentString(SparkWordCount.processWordCount().mkString("\n"))
    resp
  }

  def greeting(s: String, resp: Response): Response = {
    resp.setContentString(s)
    resp
  }

  val service = new Service[Request, Response] {
    def apply(req: Request): Future[Response] = {
      val resp = Response(req.version, Status.Ok)
      req.path match {

        // Will do the "du -h" bash command
        case "/bash" => {
          Future.value(
            {
              val proc = Process("du -h")
              val output = proc.lineStream.toList.mkString("\n")
              resp.setContentString(output)
              resp
            })
        }

        // Sample Spark WordCount
        case "/process" => {
          Future.value(process(resp))
        }

        // Sample Greeting
        case "/greetings" => {
          Future.value(greeting("This is the greeting", resp))
        }

        // Sample default output
        case _ =>
          Future.value(resp)
      }
    }
  }

  val server = Http.serve(":40000", service)
  Await.ready(server)
}

