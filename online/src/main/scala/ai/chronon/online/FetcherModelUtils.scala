package ai.chronon.online

import ai.chronon.api.Extensions.{ModelTransformOps, ThrowableOps}
import ai.chronon.api.ModelTransform
import ai.chronon.online.Fetcher.ResponseWithContext
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future}

// Unique identifier for a model request, used to join inference result back to the original join request
case class ModelRequestIdentifier(joinName: String, modelName: String, prefix: Option[String]) {
  def stringRep: String = (Seq(joinName) ++ prefix.toSeq ++ Seq(modelName)).mkString("_")
}
case class ModelTransformRequest(model: ModelTransform,
                                 modelRequestIdentifier: ModelRequestIdentifier,
                                 inputs: Map[String, AnyRef])
case class ModelTransformResponse(modelRequestIdentifier: ModelRequestIdentifier,
                                  outputs: Map[String, AnyRef],
                                  exceptions: Map[String, Throwable])

object FetcherModelUtils {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /*
   * Convert each join request into a list of ModelTransformRequest, with ModelRequestIdentifier as join key
   */
  private def buildModelTransformRequests(joinRequest: ResponseWithContext): Seq[ModelTransformRequest] = {
    if (joinRequest.joinCodec.isEmpty) {
      Seq.empty
    } else {
      val joinName = joinRequest.joinCodec.get.conf.join.metaData.name
      val requests = joinRequest.joinCodec.get.conf.modelTransformsListScala.map { modelTransform =>
        val modelName = modelTransform.model.metaData.name
        val prefix = Option(modelTransform.prefix)
        ModelTransformRequest(modelTransform,
                              ModelRequestIdentifier(joinName, modelName, prefix),
                              joinRequest.derivedValues.get)
      }
      requests
    }
  }

  /*
   * Given a list of ModelTransformRequest, run inference via ModelBackend, handling input/output mapping in the process
   */
  private def runModelTransforms(modelRequests: Seq[ModelTransformRequest], modelBackend: ModelBackend)(implicit
      executionContext: ExecutionContext): Future[Seq[ModelTransformResponse]] = {
    val requestsByModel = modelRequests.groupBy(_.model)
    val futures = requestsByModel.map {
      case (modelTransform, requests) =>
        val identifiers = requests.map(_.modelRequestIdentifier)
        val modelName = modelTransform.model.metaData.name
        val mappedInputs = requests.map(_.inputs).map(modelTransform.mapInputs)
        val joins = identifiers.map(_.joinName).distinct.sorted.mkString(",")
        val ctx = Metrics.Context(environment = Metrics.Environment.ModelTransform, join = joins, model = modelName)
        val startTs = System.currentTimeMillis()
        ctx.increment(Metrics.Name.RequestCount)
        val runModelInferenceRequest = RunModelInferenceRequest(modelTransform.model, mappedInputs)
        val runModelInferenceResponseFuture = modelBackend.runModelInference(runModelInferenceRequest)
        val modelResponses = runModelInferenceResponseFuture
          .map { resp =>
            val endTs = System.currentTimeMillis()
            ctx.distribution(Metrics.Name.LatencyMillis, endTs - startTs)
            resp
          }
          .map(_.outputs)
          .map { outputs =>
            outputs.zip(requests).map {
              case (outputs, modelRequest) =>
                val modelRequestIdentifier = modelRequest.modelRequestIdentifier
                val mappedOutputs = modelTransform.mapOutputs(outputs)
                ModelTransformResponse(modelRequestIdentifier, mappedOutputs, Map.empty)
            }
          }
          .recover {
            case e: Throwable =>
              ctx.incrementException(e)
              identifiers.map { identifier =>
                ModelTransformResponse(identifier, Map.empty, Map(identifier.stringRep + "_exception" -> e))
              }
          }
        modelResponses
    }
    Future.sequence(futures).map(_.flatten.toSeq)
  }

  /*
   * Entry point to the ModelTransform stage, which takes in previous stage's output (derivations), run through
   * model transforms via ModelBackend, and return the results
   */
  def fetchModelTransforms(joinRequestsFuture: Future[scala.collection.Seq[ResponseWithContext]],
                           modelBackend: ModelBackend)(implicit
      executionContext: ExecutionContext): Future[scala.collection.Seq[ResponseWithContext]] = {

    val modelTransformsStartTs = System.currentTimeMillis()
    joinRequestsFuture.flatMap { joinRequests =>
      val hasModelTransforms = joinRequests.flatMap(_.joinCodec).exists(_.conf.hasModelTransforms)
      if (!hasModelTransforms) {
        Future.successful(joinRequests)
      } else {
        // Step1: For each join request, build a list of ModelTransformRequest, with ModelRequestIdentifier as join key
        val modelTransformRequestsByJoinRequest = joinRequests.map { req =>
          val modelTransformRequests = (req, buildModelTransformRequests(req))
          val modelNames =
            modelTransformRequests._2.map(_.modelRequestIdentifier.modelName).distinct.sorted.mkString(",")
          req.ctx
            .withSuffix(Metrics.Environment.ModelTransform)
            .copy(model = modelNames)
            .increment(Metrics.Name.RequestCount)
          modelTransformRequests

        }
        val modelTransformRequests = modelTransformRequestsByJoinRequest.flatMap(_._2)

        // Step2. For all ModelTransformRequest, run inference in parallel using ModelBackend, and group by identifier
        val modelTransformResponsesFuture =
          runModelTransforms(modelTransformRequests, modelBackend)
        val modelTransformResponsesByIdentifierFuture = modelTransformResponsesFuture.map { responses =>
          responses.map { resp => (resp.modelRequestIdentifier, resp) }.toMap
        }

        // Step 3: For each join request, join ModelTransformResponse back to request using identifier as join key
        val postModelTransformsFuture = modelTransformResponsesByIdentifierFuture.map { respByIdentifier =>
          modelTransformRequestsByJoinRequest.map {
            case (req, modelTransformRequests) =>
              val identifiers = modelTransformRequests.map(_.modelRequestIdentifier)
              val modelNames = identifiers.map(_.modelName).distinct.mkString(",")
              val modelTransformResponses = identifiers
                .flatMap { id =>
                  // Look up response by identifier
                  val respOpt = respByIdentifier.get(id)
                  respOpt.toSeq.flatMap(_.exceptions.values).foreach { ex =>
                    req.ctx
                      .withSuffix(Metrics.Environment.ModelTransform)
                      .copy(model = id.modelName)
                      .incrementException(ex)
                  }
                  respOpt.map { resp =>
                    // Return exceptions as part of the response to caller
                    resp.outputs ++ resp.exceptions.mapValues(_.traceString)
                  }
                }
                .reduce(_ ++ _) // union all modelTransforms result together

              // Handle passthrough values
              val passthroughValues = req.joinCodec.toSeq
                .flatMap(_.conf.modelTransformsPassthroughFieldsScala)
                .map { fieldName =>
                  fieldName -> req.derivedValues.getOrElse(fieldName, null)
                }
                .toMap

              val updatedJoinRequest =
                req.copy(modelTransformsValues = Some(modelTransformResponses ++ passthroughValues))
              val requestEndTs = System.currentTimeMillis()
              req.ctx
                .withSuffix(Metrics.Environment.ModelTransform)
                .copy(model = modelNames)
                .distribution(Metrics.Name.LatencyMillis, requestEndTs - modelTransformsStartTs)
              updatedJoinRequest
          }
        }
        postModelTransformsFuture
      }
    }
  }
}
