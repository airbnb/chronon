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
    val responsesByNameF = requests
      .groupBy(_.name) // groupBy end point
      //.par // issue requests in parallel (might not be necessary if the executor parallelizes)
      .map {
        case (name, requests) =>
          if (handlerMap.contains(name)) {
            handlerMap(name).fetch(requests) // issue batch request to endpoint
          } else {
            val failure = Failure(
              new IllegalArgumentException(
                s"$name is not registered among handlers: [${handlerMap.keys.mkString(", ")}]"))
            Future(requests.map(request => Response(request, failure)))
          }
      }
    //.seq
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
