package org.minyodev.finagle

import com.twitter.finagle.{Http, Service, http}
import com.twitter.util.{Await, Future}

/**
  * @author aesguerra
  * */
object SimpleClient extends App {

  val client: Service[http.Request, http.Response] = Http.newService(":40000")
  // Sample of Scala HTTP Request and Response here
  val request = http.Request(http.Method.Get, "/command")
  val response: Future[http.Response] = client(request)
  response.onSuccess(resp => println(resp.getContentString()))
  Await.ready(response)
  Thread.sleep(1000)
}