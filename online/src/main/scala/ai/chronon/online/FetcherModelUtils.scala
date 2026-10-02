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
import ai.chronon.api.{Model, ModelTransform}
import ai.chronon.online.Fetcher.ResponseWithContext
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Random, Try}

object FetcherModelUtils {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Keys read from `model.inferenceSpec.modelBackendParams`. All default to the historical behavior. */
  object InferenceParams {
    // Retries after the first failed attempt. Historical value: 2 immediate retries.
    val MaxRetries = "chronon_inference_max_retries"
    // Delay before the first retry; doubles per retry up to MaxBackoffMs. 0 keeps retries immediate.
    val BackoffMs = "chronon_inference_retry_backoff_ms"
    val MaxBackoffMs = "chronon_inference_retry_max_backoff_ms"
    // Comma-separated substrings; a failure whose cause chain has a class name or message containing
    // one of them is not retried. Meant for back-pressure and rate-limit responses, where a retry
    // only adds load to a backend that has just asked for less of it.
    val NonRetryableExceptions = "chronon_inference_non_retryable_exceptions"
    // Inputs per backend call. 0 sends every deduped input for a model in one merged request, so
    // one failure nulls the model outputs of every row in the partition; a positive value isolates
    // failures to the chunk they happened in.
    val ChunkSize = "chronon_inference_chunk_size"
  }

  private[online] case class RetryPolicy(maxRetries: Int,
                                         backoffMs: Long,
                                         maxBackoffMs: Long,
                                         nonRetryablePatterns: Seq[String]) {
    def isRetryable(e: Throwable): Boolean = {
      if (nonRetryablePatterns.isEmpty) return true
      val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16)
      !causes.exists { t =>
        val name = t.getClass.getName
        val message = Option(t.getMessage).getOrElse("")
        nonRetryablePatterns.exists(p => name.contains(p) || message.contains(p))
      }
    }

    /** Exponential backoff with up to 20% jitter, so retries from many partitions do not align. */
    def delayMsFor(attempt: Int): Long = {
      if (backoffMs <= 0) return 0L
      val base = math.min(maxBackoffMs, backoffMs * (1L << math.min(attempt, 30)))
      base + (Random.nextDouble() * math.max(1L, base / 5)).toLong
    }
  }

  private[online] object RetryPolicy {
    val default: RetryPolicy =
      RetryPolicy(maxRetries = 2, backoffMs = 0, maxBackoffMs = 0, nonRetryablePatterns = Seq.empty)

    def fromParams(params: Map[String, String]): RetryPolicy = {
      def long(key: String, fallback: Long): Long =
        params.get(key).flatMap(v => Try(v.trim.toLong).toOption).getOrElse(fallback)
      val backoff = long(InferenceParams.BackoffMs, default.backoffMs)
      RetryPolicy(
        maxRetries = long(InferenceParams.MaxRetries, default.maxRetries).toInt,
        backoffMs = backoff,
        maxBackoffMs = long(InferenceParams.MaxBackoffMs, backoff * 10),
        nonRetryablePatterns = params
          .get(InferenceParams.NonRetryableExceptions)
          .toSeq
          .flatMap(_.split(","))
          .map(_.trim)
          .filter(_.nonEmpty)
      )
    }

    def fromModel(model: Model): RetryPolicy = fromParams(backendParams(model))
  }

  private[online] def backendParams(model: Model): Map[String, String] = {
    import scala.util.ScalaJavaConversions.MapOps
    Option(model.inferenceSpec).flatMap(s => Option(s.modelBackendParams)).map(_.toScala).getOrElse(Map.empty)
  }

  private[online] def inferenceChunkSize(model: Model): Int =
    backendParams(model).get(InferenceParams.ChunkSize).flatMap(v => Try(v.trim.toInt).toOption).getOrElse(0)

  /** One daemon thread that only fires delayed retries; the retried call itself runs on the caller's context. */
  private object RetryScheduler {
    lazy val executor: ScheduledThreadPoolExecutor = {
      val factory = new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, "chronon-inference-retry")
          t.setDaemon(true)
          t
        }
      }
      new ScheduledThreadPoolExecutor(1, factory)
    }

    def after[T](delayMs: Long)(f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      if (delayMs <= 0) f
      else {
        val promise = Promise[T]()
        executor.schedule(new Runnable {
                            override def run(): Unit = promise.completeWith(Future(f).flatMap(identity))
                          },
                          delayMs,
                          TimeUnit.MILLISECONDS)
        promise.future
      }
    }
  }

  private[online] def withRetry[T](policy: RetryPolicy, attempt: Int = 0)(f: => Future[T])(implicit
      ec: ExecutionContext): Future[T] = {
    f.recoverWith {
      case e: Throwable if attempt < policy.maxRetries && policy.isRetryable(e) =>
        val delayMs = policy.delayMsFor(attempt)
        logger.warn(
          s"Model inference failed, retrying in ${delayMs}ms (${policy.maxRetries - attempt - 1} retries left after this): ${e.getMessage}")
        RetryScheduler.after(delayMs)(withRetry(policy, attempt + 1)(f))
      case e: Throwable if attempt < policy.maxRetries =>
        logger.warn(s"Model inference failed with a non-retryable error, not retrying: ${e.getMessage}")
        Future.failed(e)
    }
  }

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
    val futures = requestsByModel.flatMap {
      case (model, requests) =>
        val policy = RetryPolicy.fromModel(model)
        val chunkSize = inferenceChunkSize(model)
        // Each entry carries one deduped input, so chunking here bounds the inputs per backend call
        // and the rows that share one failure. chunkSize <= 0 keeps the single merged request.
        val chunks: Seq[Seq[(RunModelInferenceRequest, Seq[ModelTransformContext])]] =
          if (chunkSize > 0) requests.grouped(chunkSize).toList else Seq(requests)
        chunks.map { chunk =>
          runChunkAndCollectResults(model, chunk, policy, modelBackend)
        }
    }

    Future.sequence(futures)
  }

  private def runChunkAndCollectResults(
      model: Model,
      requests: Seq[(RunModelInferenceRequest, Seq[ModelTransformContext])],
      policy: RetryPolicy,
      modelBackend: ModelBackend
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val mergedRequest = requests.map(_._1).reduce(_ merge _)
    val modelTransformContexts: Seq[Seq[ModelTransformContext]] = requests.map(_._2)

    val modelName = model.metaData.name
    // Build the metric tag for join by simply concatenating all join names
    val joins = modelTransformContexts.flatten.map(_.joinName).distinct.sorted.mkString(",")
    val ctx = Metrics.Context(environment = Metrics.Environment.ModelTransform, join = joins, model = modelName)
    val startTs = System.currentTimeMillis()
    ctx.increment(Metrics.Name.RequestCount)
    ctx.increment(Metrics.Name.RequestBatchSize)
    ctx.distribution(Metrics.Name.InferenceInputCount, mergedRequest.inputs.size)

    withRetry(policy)(modelBackend.runModelInference(mergedRequest))
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
