/*
 *    Copyright (C) 2025 The Chronon Authors.
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

import ai.chronon.api.Extensions.ModelTransformOps
import ai.chronon.api.ModelTransform
import ai.chronon.online.Fetcher.ResponseWithContext
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}

object FetcherModelUtils {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private case class ModelTransformContext(joinName: String, resultMap: mutable.HashMap[String, AnyRef])
  private case class ModelTransformInput(modelTransform: ModelTransform,
                                         inputs: Map[String, AnyRef],
                                         context: ModelTransformContext)

  /*
   * For each join request, builds a list of ModelTransformInputs based on model transforms metadata and
   * derivation output, and also builds a mutable result map in order to collect results.
   */
  private def buildModelTransformInputs(joinRequest: ResponseWithContext): Seq[ModelTransformInput] = {
    if (joinRequest.joinCodec.isEmpty) {
      Seq.empty
    } else {
      val requests = joinRequest.joinCodec.get.conf.modelTransformsListScala.map { modelTransform =>
        val mappedInputs = modelTransform.mapInputs(joinRequest.derivedValues.get)
        ModelTransformInput(
          modelTransform,
          mappedInputs,
          ModelTransformContext(
            joinRequest.joinCodec.get.conf.join.metaData.name,
            // Initialize a mutable HashMap to collect results later on
            new mutable.HashMap[String, AnyRef]()
          )
        )
      }
      requests
    }
  }

  /*
   * Given a list of join requests and their model transform inputs/contexts, this method builds the
   * RunModelInferenceRequests (inputs to ModelBackend) and keep track of the contexts.
   */
  private def buildUniqueRunModelInferenceRequestsWithContexts(
      joinToModelTransformInputs: Seq[(ResponseWithContext, Seq[ModelTransformInput])]
  ): Seq[(RunModelInferenceRequest, Seq[ModelTransformContext])] = {

    joinToModelTransformInputs
      .flatMap {
        case (_, modelTransformInputs) =>
          modelTransformInputs
            .map { input =>
              // Convert ModelTransformInput to RunModelInferenceRequest
              val model = input.modelTransform.model
              val mappedInputs = input.inputs
              val runModelInferenceRequest = RunModelInferenceRequest(model, Seq(mappedInputs))
              (runModelInferenceRequest, input.context)
            }
      }
      // This will dedupe RunModelInferenceRequests and map each unique RunModelInferenceRequest
      // to the list of ModelTransformContexts that need to be updated
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .toSeq
  }

  /*
   * Given a list of RunModelInferenceRequests and their backtracked result map list, this method
   * first re-groups the requests by model, then runs model inference using model backend, and finally
   * writes the results back to the mutable result maps.
   */
  private def runModelInferenceRequestsAndCollectResults(
      runModelInferenceRequestsWithResultMap: Seq[(RunModelInferenceRequest, Seq[ModelTransformContext])],
      modelBackend: ModelBackend
  )(implicit executionContext: ExecutionContext): Future[Iterable[Unit]] = {

    // First, group by model because each model inference call can handle only 1 model
    val requestsByModel = runModelInferenceRequestsWithResultMap.groupBy(_._1.model)
    val futures = requestsByModel.map {
      case (model, requests) =>
        val mergedRequest = requests.map(_._1).reduce(_ merge _)
        val modelTransformContexts: Seq[Seq[ModelTransformContext]] = requests.map(_._2)

        val modelName = model.metaData.name
        // Build the metric tag for join by simply concatenating all join names
        val joins = modelTransformContexts.flatten.map(_.joinName).distinct.sorted.mkString(",")
        val ctx = Metrics.Context(environment = Metrics.Environment.ModelTransform, join = joins, model = modelName)
        val startTs = System.currentTimeMillis()
        ctx.increment(Metrics.Name.RequestCount)
        ctx.increment(Metrics.Name.RequestBatchSize)

        modelBackend
          .runModelInference(mergedRequest)
          .map { response =>
            assert(
              response.outputs.size == modelTransformContexts.size,
              s"Model $modelName returned ${response.outputs.size} outputs, but expected ${modelTransformContexts.size} outputs for joins: $joins"
            )
            ctx.increment(Metrics.Name.ResponseCount)
            response.outputs.zip(modelTransformContexts)
          }
          .recover {
            case e: Throwable =>
              ctx.incrementException(e)
              val exceptionMap = Map(model.metaData.name + "_exception" -> e)
              modelTransformContexts.map { modelTransformContext =>
                // For each model transform context, we return an exception in the result map
                (exceptionMap, modelTransformContext)
              }
          }
          .map { zippedOutputs =>
            zippedOutputs.iterator.foreach {
              case (outputs, modelTransformContexts) =>
                outputs.foreach {
                  case (key, value) =>
                    // For each output, update all result maps
                    modelTransformContexts.foreach(_.resultMap.put(key, value))
                }
            }

            // Instrument model transform latency
            ctx.distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTs)
          }
    }

    Future.sequence(futures)
  }

  /*
   * Given the original list of join requests and the mutable result maps (already populated at this stage),
   * this method processes everything and produces the final output: handle output mappings and passthrough fields.
   */
  private def finalizeOutputs(
      joinToModelTransformInputs: Seq[(ResponseWithContext, Seq[ModelTransformInput])]
  ): Seq[ResponseWithContext] = {
    joinToModelTransformInputs.map {
      case (joinRequest, modelTransformInputs) =>
        val mappedOutputs = modelTransformInputs
          .map { input =>
            val modelTransform = input.modelTransform
            val resultMap = input.context.resultMap.toMap
            // Apply output mappings
            val mappedOutputs = modelTransform.mapOutputs(resultMap)
            mappedOutputs
          }
          .reduceOption(_ ++ _) // Combine all outputs from different model transforms
          .getOrElse(Map.empty[String, AnyRef])

        // Add passthrough values
        val passthroughValues = joinRequest.joinCodec.toSeq
          .flatMap(_.conf.modelTransformsPassthroughFieldsScala)
          .map { fieldName =>
            fieldName -> joinRequest.derivedValues.getOrElse(fieldName, null)
          }
          .toMap

        joinRequest.copy(modelTransformsValues = Some(mappedOutputs ++ passthroughValues))
    }
  }

  /*
   * Main entry point: Given a list of join requests, fetch model transforms for each request.
   */
  def fetchModelTransforms(joinRequestsFuture: Future[scala.collection.Seq[ResponseWithContext]],
                           modelBackend: ModelBackend)(implicit
      executionContext: ExecutionContext): Future[scala.collection.Seq[ResponseWithContext]] = {
    val startTs = System.currentTimeMillis()
    joinRequestsFuture.flatMap { joinRequests =>
      val hasModelTransforms = joinRequests.flatMap(_.joinCodec).exists(_.conf.hasModelTransforms)
      if (!hasModelTransforms) {
        Future.successful(joinRequests)
      } else {

        // Build the metric tag for model by simply concatenating all model names
        val modelNames = joinRequests
          .flatMap(_.joinCodec)
          .flatMap(_.conf.modelTransformsListScala)
          .map(_.model.metaData.name)
          .distinct
          .sorted
          .mkString(",")
        joinRequests.iterator.foreach { req =>
          req.ctx
            .withSuffix(Metrics.Environment.ModelTransform)
            .copy(model = modelNames)
            .increment(Metrics.Name.RequestCount)
        }

        // Step1: For each join request, for each ModelTransform, apply input mappings and build a mutable result map
        // per each Join request + model transform level, which will be used to collect inference results
        val joinToModelTransformInputs: Seq[(ResponseWithContext, Seq[ModelTransformInput])] =
          joinRequests.map(req => (req, buildModelTransformInputs(req)))

        // Step2: Convert each ModelTransformInput to RunModelInferenceRequest, dedupe based on model/keys,
        // and backtrack the list of mutable result maps that need to be updated
        val runModelInferenceRequestsWithResultMap: Seq[(RunModelInferenceRequest, Seq[ModelTransformContext])] =
          buildUniqueRunModelInferenceRequestsWithContexts(joinToModelTransformInputs)

        // Step3: Run ModelInferenceRequests using ModelBackend. This step also collects result from model inference
        // write results back to mutable result map at Join + model transform level
        val runModelInferenceRequestsFuture = runModelInferenceRequestsAndCollectResults(
          runModelInferenceRequestsWithResultMap,
          modelBackend
        )

        // Step4: For each join request, for each ModelTransform, apply output mappings and passthroughs on
        // collected result map
        val joinResponsesFuture = runModelInferenceRequestsFuture.map {
          // Note: the future's return value is ignored, but the processing needs to wait till the future finishes
          // which ensures that the result maps are all updated
          _ => finalizeOutputs(joinToModelTransformInputs)
        }

        // Instrumentation for latency and exceptions
        joinResponsesFuture.foreach { joinResponses =>
          joinResponses.iterator.foreach { resp =>
            val ctx = resp.ctx
              .withSuffix(Metrics.Environment.ModelTransform)
              .copy(model = modelNames)
            ctx.distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTs)

            resp.modelTransformsValues.iterator.foreach(_.iterator.foreach {
              case (key, value) =>
                if (key.endsWith("_exception")) {
                  ctx.incrementException(value.asInstanceOf[Throwable])
                }
            })
          }
        }

        joinResponsesFuture
      }
    }
  }
}
