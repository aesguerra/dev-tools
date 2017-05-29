package org.minyodev.examples.finagle

import java.net.URLEncoder

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http
import com.twitter.finagle.http.RequestBuilder
import com.twitter.util.{Await, Future}

/**
  * @author aesguerra
  * */
object HelloClient extends App {

  val client: Service[http.Request, http.Response] = Http.newService(":40000")
  def buildUri(base: String, path: String, params: Map[String, String] = Map.empty): String = {
    val p = if (params.isEmpty) ""
    else params map { case (k,v) => urlEncode(k) + "=" + urlEncode(v) } mkString ("?", "&", "")
    val out = base + path + p
    println("Browsing: " + out)
    out
  }

  def urlEncode(url: String): String = URLEncoder.encode(url, "UTF-8")

  val url = buildUri("http://localhost", ":40000", Map("username" -> "ana", "sex" -> "f", "age" -> "18"))
  val req = RequestBuilder().url(url).setHeader("Accept", "*/*").buildGet
  val resp = client(req)
  resp.onSuccess(r => {
    println(r.getStatusCode())
    println(r.getContentString())
  })
  Await.ready(resp)

  Thread.sleep(1000)
}