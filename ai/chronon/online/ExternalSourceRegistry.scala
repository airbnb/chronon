/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.api.Constants
import ai.chronon.online.Fetcher.{Request, Response}

import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.ScalaJavaConversions.IterableOps
import scala.util.{Failure, Success}

// users can simply register external endpoints with a lambda that can return the future of a response given keys
// keys and values need to match schema in ExternalSource - chronon will validate automatically
class ExternalSourceRegistry extends Serializable {
  class ContextualHandler extends ExternalSourceHandler {
    override def fetch(requests: Seq[Request]): Future[Seq[Response]] = {
      Future(requests.map { request =>
        Response(request = request, values = Success(request.keys))
      })
    }
  }

  val handlerMap: mutable.Map[String, ExternalSourceHandler] = {
    val result = new mutable.HashMap[String, ExternalSourceHandler]()
    result.put(Constants.ContextualSourceName, new ContextualHandler())
    result
  }

  def add(name: String, handler: ExternalSourceHandler): Unit = {
    assert(!handlerMap.contains(name),
           s"A handler by the name $name already exists. Existing: ${handlerMap.keys.mkString("[", ", ", "]")}")
    handlerMap.put(name, handler)
  }

  // TODO: validation of externally fetched data
  // 1. keys match
  // 2. report missing & extra values
  // 3. schema integrity of returned values
  def fetchRequests(requests: Seq[Request], context: Metrics.Context)(implicit
      ec: ExecutionContext): Future[Seq[Response]] = {
    val startTime = System.currentTimeMillis()
    // we make issue one batch request per external source and flatten out it later
    val responsesByNameF: List[Future[Seq[Response]]] = requests
      .groupBy(_.name)
      .map {
        case (name, requests) =>
          if (handlerMap.contains(name)) {
            val ctx = context.copy(groupBy = s"${Constants.ExternalPrefix}_$name")
            val responses = handlerMap(name).fetch(requests)
            responses.map { responses =>
              val failures = responses.count(_.values.isFailure)
              ctx.distribution("response.latency", System.currentTimeMillis() - startTime)
              ctx.count("response.failures", failures)
              ctx.count("response.successes", responses.size - failures)
              responses
            }
          } else {
            val failure = Failure(
              new IllegalArgumentException(
                s"$name is not registered among handlers: [${handlerMap.keys.mkString(", ")}]"))
            Future(requests.map(request => Response(request, failure)))
          }
      }
      .toList

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
