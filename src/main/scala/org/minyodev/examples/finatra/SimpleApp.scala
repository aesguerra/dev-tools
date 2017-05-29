package org.minyodev.examples.finatra

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller, HttpServer}
import com.twitter.finatra.http.routing.HttpRouter

object FitmanApp extends FitmanServer

class FitmanServer extends HttpServer {
  override protected def configureHttp(router: HttpRouter): Unit = {
    router.add(new HelloController)
  }
}

class HelloController extends Controller {
  get("/hello") { request: Request =>
    "Fitman says hello"
  }
}