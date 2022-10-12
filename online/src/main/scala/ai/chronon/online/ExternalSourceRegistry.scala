package ai.chronon.online

import ai.chronon.online.Fetcher.{Request, Response}

import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

// users can simply register external endpoints with a lambda that can return the future of a response given keys
// keys and values need to match schema in ExternalSource - chronon will validate automatically
class ExternalSourceRegistry {
  val handlerMap: mutable.Map[String, ExternalSourceHandler] =
    new mutable.HashMap[String, ExternalSourceHandler]()

  def add(name: String, handler: ExternalSourceHandler): Unit = {
    handlerMap.put(name, handler)
  }

  def fetchRequests(requests: Seq[Request], context: Metrics.Context)(implicit
      ec: ExecutionContext): Future[Seq[Response]] = {
    val startTime = System.currentTimeMillis()
    // we make issue one batch request per external source and flatten out it later
    val responsesByNameF: Iterable[Future[Seq[Response]]] = requests
      .groupBy(_.name)
      .map {
        case (name, requests) =>
          if (handlerMap.contains(name)) {
            val ctx = context.copy(groupBy = s"external_source_$name")
            val responses = handlerMap(name).fetch(requests)
            responses.foreach { responses =>
              val failures = responses.count(_.values.isFailure)
              ctx.histogram("response.latency", System.currentTimeMillis() - startTime)
              ctx.histogram("response.failures", failures)
              ctx.histogram("response.successes", responses.size - failures)
            } // issue batch request to endpoint
            responses
          } else {
            val failure = Failure(
              new IllegalArgumentException(
                s"$name is not registered among handlers: [${handlerMap.keys.mkString(", ")}]"))
            Future(requests.map(request => Response(request, failure)))
          }
      }

    Future.sequence(responsesByNameF).map { responsesByName =>
      val allResponses = responsesByName.flatten
      requests.map(req =>
        allResponses
          .find(_.request == req)
          .getOrElse(Response(
            req,
            Failure( // logic error - some handler missed returning a response
              new IllegalStateException(s"Missing response for request $req among \n $allResponses"))
          )))
    }
  }
}
