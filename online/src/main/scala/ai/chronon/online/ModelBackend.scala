package ai.chronon.online

import ai.chronon.api.{Join, Model}

import scala.collection.Seq
import scala.concurrent.Future

case class RunModelInferenceRequest(model: Model, inputs: Seq[Map[String, AnyRef]])
case class RunModelInferenceResponse(request: RunModelInferenceRequest, outputs: Seq[Map[String, AnyRef]])

trait ModelBackend {

  def runModelInference(runModelInferenceRequest: RunModelInferenceRequest): Future[RunModelInferenceResponse]
  // Run online model inference which returns the model output directly.
  // Will be used in fetcher.fetchJoin
}
