package ai.chronon.online

import ai.chronon.api.Extensions.{ModelTransformOps, ThrowableOps}
import ai.chronon.api.ModelTransform
import ai.chronon.online.Fetcher.ResponseWithContext

import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future}

case class ModelRequestIdentifier(joinName: String, modelName: String, prefix: Option[String]) {
  def stringRep: String = (Seq(joinName) ++ prefix.toSeq ++ Seq(modelName)).mkString("_")
}
case class ModelTransformRequest(model: ModelTransform,
                                 modelRequestIdentifier: ModelRequestIdentifier,
                                 inputs: Map[String, AnyRef])
case class ModelTransformResponse(modelRequestIdentifier: ModelRequestIdentifier, outputs: Map[String, AnyRef])

object FetcherModelUtils {
  private def buildModelTransformRequests(joinRequest: ResponseWithContext): Seq[ModelTransformRequest] = {
    if (joinRequest.joinCodec.isEmpty) {
      Seq.empty
    } else {
      val joinName = joinRequest.joinCodec.get.conf.join.metaData.name
      val requests = joinRequest.joinCodec.get.conf.modelTransformsScala.map { modelTransform =>
        val modelName = modelTransform.model.metaData.name
        val prefix = Option(modelTransform.prefix)
        ModelTransformRequest(modelTransform,
                              ModelRequestIdentifier(joinName, modelName, prefix),
                              joinRequest.derivedValues)
      }
      requests
    }
  }

  private def runModelTransforms(modelRequests: Seq[ModelTransformRequest], modelBackend: ModelBackend)(implicit
      executionContext: ExecutionContext): Future[Seq[ModelTransformResponse]] = {
    val requestsByModel = modelRequests.groupBy(_.model)
    val futures = requestsByModel.map {
      case (modelTransform, requests) =>
        val identifiers = requests.map(_.modelRequestIdentifier)
        val mappedInputs = requests.map(_.inputs).map(modelTransform.mapInputs)
        val future =
          modelBackend.runModelInference(RunModelInferenceRequest(modelTransform.model, mappedInputs)).map(_.outputs)
        val modelResponses = future
          .map { outputs =>
            outputs.zip(requests).map {
              case (outputs, modelRequest) =>
                val modelRequestIdentifier = modelRequest.modelRequestIdentifier
                val mappedOutputs = modelTransform.mapOutputs(outputs, modelRequest.inputs)
                ModelTransformResponse(modelRequestIdentifier, mappedOutputs)
            }
          }
          .recover {
            case e: Throwable =>
              identifiers.map { identifier =>
                ModelTransformResponse(identifier, Map(identifier.stringRep + "_exception" -> e.traceString))
              }
          }
        modelResponses
    }
    Future.sequence(futures).map(_.flatten.toSeq)
  }

  def fetchModelTransforms(joinRequestsFuture: Future[scala.collection.Seq[ResponseWithContext]],
                           modelBackend: ModelBackend)(implicit
      executionContext: ExecutionContext): Future[scala.collection.Seq[ResponseWithContext]] =
    joinRequestsFuture.flatMap { joinRequests =>
      val hasModelTransforms = joinRequests.flatMap(_.joinCodec).exists(_.conf.hasModelTransforms)
      if (!hasModelTransforms) {
        Future.successful(joinRequests)
      } else {
        // 1. For each join request, build a list of ModelTransformRequest, with ModelRequestIdentifier as join key
        // 2. For all ModelTransformRequest, run inference in parallel using ModelBackend
        // 3. For each join request, collect results from ModelTransformResponse using ModelRequestIdentifier as join key
        val modelTransformRequestsByJoinRequest = joinRequests.map(req => (req, buildModelTransformRequests(req)))
        val modelTransformRequests = modelTransformRequestsByJoinRequest.flatMap(_._2)
        val modelTransformResponsesFuture = runModelTransforms(modelTransformRequests, modelBackend)
        val modelTransformResponsesByIdentifierFuture = modelTransformResponsesFuture.map { responses =>
          responses.map { resp => (resp.modelRequestIdentifier, resp.outputs) }.toMap
        }
        val postModelTransformsFuture = modelTransformResponsesByIdentifierFuture.map {
          modelTransformResponsesByIdentifier =>
            modelTransformRequestsByJoinRequest.map {
              case (joinRequest, modelTransformRequests) =>
                val modelTransformResponses =
                  modelTransformRequests
                    .map(_.modelRequestIdentifier)
                    .flatMap(modelTransformResponsesByIdentifier.get)
                    .reduce(_ ++ _) // Collect result by looking up using identifier as key
                joinRequest.copy(
                  modelTransformsValues = Some(modelTransformResponses)
                )
            }
        }
        postModelTransformsFuture
      }
    }
}
