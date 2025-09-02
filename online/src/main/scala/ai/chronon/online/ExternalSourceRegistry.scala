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

import ai.chronon.api.{Constants, ExternalPart}
import ai.chronon.online.Fetcher.{Request, Response}

import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
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

  val handlerFactoryMap: mutable.Map[String, ExternalSourceFactory] = {
    val result = new mutable.HashMap[String, ExternalSourceFactory]()
    result
  }

  def add(name: String, handler: ExternalSourceHandler): Unit = {
    assert(!handlerMap.contains(name),
           s"A handler by the name $name already exists. Existing: ${handlerMap.keys.mkString("[", ", ", "]")}")
    handlerMap.put(name, handler)
  }

  def addFactory(factoryName: String, factory: ExternalSourceFactory): Unit = {
    assert(
      !handlerFactoryMap.contains(factoryName),
      s"A factory by the name $factoryName already exists. Existing: ${handlerFactoryMap.keys.mkString("[", ", ", "]")}"
    )
    handlerFactoryMap.put(factoryName, factory)
  }

  /**
    * Add external source handlers using factory from external parts.
    * This function processes external parts, extracts FactoryConfig information
    * from external sources, and creates handlers using registered factories.
    *
    * @param externalParts Optional sequence of ExternalPart objects to process
    */
  def addHandlersByFactory(externalParts: Option[Seq[ExternalPart]]): Unit = {
    try {
      // Check if external parts are provided
      externalParts.foreach { externalParts =>
        externalParts.foreach { externalPart =>
          val externalSource = externalPart.getSource
          val sourceName = externalSource.getMetadata.getName

          // Skip if handler already exists (avoid duplicate registration)
          if (!handlerMap.contains(sourceName)) {
            if (externalSource.getFactoryConfig == null) {
              // External source does not have factory configuration, skip registration
            } else {
              // Factory configuration exists, verify it has required fields
              val factoryConfig = externalSource.getFactoryConfig

              if (factoryConfig.getFactoryName == null) {
                throw new IllegalArgumentException(
                  s"External source '$sourceName' has factory configuration but factoryName is null"
                )
              }

              if (factoryConfig.getFactoryParams == null) {
                throw new IllegalArgumentException(
                  s"External source '$sourceName' has factory configuration but factoryParams is null"
                )
              }

              val factoryName = factoryConfig.getFactoryName

              // External source has valid factory configuration, check if factory is registered
              handlerFactoryMap.get(factoryName) match {
                case Some(factory) =>
                  // Factory is registered, create and add handler
                  val handler = factory.createExternalSourceHandler(externalSource)
                  add(sourceName, handler)
                case None =>
                  // Factory is not registered, throw error
                  throw new IllegalArgumentException(
                    s"Factory '$factoryName' is not registered in ExternalSourceRegistry. " +
                      s"Available factories: [${handlerFactoryMap.keys.mkString(", ")}]"
                  )
              }
            }
          }
        }
      }
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  // TODO: validation of externally fetched data
  // 1. keys match
  // 2. report missing & extra values
  // 3. schema integrity of returned values
  def fetchRequests(requests: Seq[Request], context: Metrics.Context, externalParts: Option[Seq[ExternalPart]] = None)(
      implicit ec: ExecutionContext): Future[Seq[Response]] = {
    val startTime = System.currentTimeMillis()
    // we make issue one batch request per external source and flatten out it later
    val responsesByNameF: List[Future[Seq[Response]]] = requests
      .groupBy(_.name)
      .map {
        case (name, requests) =>
          resolveHandler(name, externalParts) match {
            case Some(handler) =>
              val ctx = context.copy(groupBy = s"${Constants.ExternalPrefix}_$name")
              val responses = handler.fetch(requests)
              responses.map { responses =>
                val failures = responses.count(_.values.isFailure)
                ctx.distribution("response.latency", System.currentTimeMillis() - startTime)
                ctx.count("response.failures", failures)
                ctx.count("response.successes", responses.size - failures)
                responses
              }
            case None =>
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

  /**
    * Resolve handler by name, attempting dynamic registration if not found.
    *
    * @param name Handler name to resolve
    * @param externalParts Optional external parts for dynamic registration
    * @return Option containing the handler if found or successfully created
    */
  private def resolveHandler(name: String, externalParts: Option[Seq[ExternalPart]]): Option[ExternalSourceHandler] = {
    handlerMap.get(name) match {
      case Some(handler) => Some(handler)
      case None          =>
        // Try dynamic registration if external parts are provided
        externalParts match {
          case Some(_) =>
            try {
              addHandlersByFactory(externalParts)
              handlerMap.get(name) // Check again after registration attempt
            } catch {
              case _: Exception => None
            }
          case None => None
        }
    }
  }
}
