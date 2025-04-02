package ai.chronon.online

import ai.chronon.api.{Join, Model}

import scala.concurrent.Future

case class RegistrationResponse(status: String, isSuccess: Boolean, message: String)
trait JobId {
  def id: String
}

trait JobStatus {
  def status: String
  def isSuccess: Boolean
  def message: String
}
trait ModelBackend {
  def registerModel(model: Model): Future[RegistrationResponse]
  // Send any required metadata to the model backend and prepare it for model inference.

  def registerModelTransform(join: Join): Future[RegistrationResponse]
  // Send any required metadata to the model backend and prepare it for (batch) model inference.

  def runModelBatchJob(join: Join, start_ds: String, end_ds: String): Future[JobId]
  // Run a batch model inference job for a given join.

  def getModelBatchJobStatus(jobId: JobId, start_ds: String, end_ds: String): Future[JobStatus]
  // Get the status of a batch model inference job.

  def runModelInference(join: Join, inputs: Seq[Map[String, AnyRef]]): Future[Seq[Map[String, AnyRef]]]
  // Run online model inference which returns the model output directly.
  // Will be used in fetcher.fetchJoin

}
