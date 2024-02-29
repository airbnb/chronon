package ai.chronon.flink

import org.slf4j.LoggerFactory
import ai.chronon.online.{Api, KVStore}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.scala.DataStream

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class WriteResponse(putRequest: PutRequest, status: Boolean)

object AsyncKVStoreWriter {
  private val kvStoreConcurrency = 10
  private val defaultTimeoutMillis = 1000L

  def withUnorderedWaits(inputDS: DataStream[PutRequest],
                         kvStoreWriterFn: RichAsyncFunction[PutRequest, WriteResponse],
                         featureGroupName: String,
                         timeoutMillis: Long = defaultTimeoutMillis,
                         capacity: Int = kvStoreConcurrency): DataStream[WriteResponse] = {
    // We use the Java API here as we have encountered issues in integration tests in the
    // past using the Scala async datastream API.
    new DataStream(
      AsyncDataStream
        .unorderedWait(
          inputDS.javaStream,
          kvStoreWriterFn,
          timeoutMillis,
          TimeUnit.MILLISECONDS,
          capacity
        )
        .uid(s"kvstore-writer-async-$featureGroupName")
        .name(s"async kvstore writes for $featureGroupName")
        .setParallelism(inputDS.parallelism)
    )
  }

  /**
    * This was moved to flink-rpc-akka in Flink 1.16 and made private, so we reproduce the direct execution context here
    */
  private class DirectExecutionContext extends ExecutionContext {
    override def execute(runnable: Runnable): Unit =
      runnable.run()

    override def reportFailure(cause: Throwable): Unit =
      throw new IllegalStateException("Error in direct execution context.", cause)

    override def prepare: ExecutionContext = this
  }

  private val ExecutionContextInstance: ExecutionContext = new DirectExecutionContext
}

/**
  * Async Flink writer function to help us write to the KV store.
  * @param onlineImpl - Instantiation of the Chronon API to help create KV store objects
  * @param featureGroupName Name of the FG we're writing to
  */
class AsyncKVStoreWriter(onlineImpl: Api, featureGroupName: String)
    extends RichAsyncFunction[PutRequest, WriteResponse] {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  @transient private var kvStore: KVStore = _

  @transient private var errorCounter: Counter = _
  @transient private var successCounter: Counter = _

  // The context used for the future callbacks
  implicit lazy val executor: ExecutionContext = AsyncKVStoreWriter.ExecutionContextInstance

  // One may want to use different KV stores depending on whether tiling is on.
  // The untiled version of Chronon works on "append" store semantics, and the tiled version works on "overwrite".
  protected def getKVStore: KVStore = {
    onlineImpl.genKvStore
  }

  override def open(configuration: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", featureGroupName)
    errorCounter = group.counter("kvstore_writer.errors")
    successCounter = group.counter("kvstore_writer.successes")

    kvStore = getKVStore
  }

  override def timeout(input: PutRequest, resultFuture: ResultFuture[WriteResponse]): Unit = {
    logger.error(s"Timed out writing to KV Store for object: $input")
    errorCounter.inc()
    resultFuture.complete(util.Arrays.asList[WriteResponse](WriteResponse(input, status = false)))
  }

  override def asyncInvoke(input: PutRequest, resultFuture: ResultFuture[WriteResponse]): Unit = {
    val resultFutureRequested: Future[Seq[Boolean]] = kvStore.multiPut(Seq(input))
    resultFutureRequested.onComplete {
      case Success(l) =>
        val succeeded = l.forall(identity)
        if (succeeded) {
          successCounter.inc()
        } else {
          errorCounter.inc()
          logger.error(s"Failed to write to KVStore for object: $input")
        }
        resultFuture.complete(util.Arrays.asList[WriteResponse](WriteResponse(input, status = succeeded)))
      case Failure(exception) =>
        // this should be rare and indicates we have an uncaught exception
        // in the KVStore - we log the exception and skip the object to
        // not fail the app
        errorCounter.inc()
        logger.error(s"Caught exception writing to KVStore for object: $input - $exception")
        resultFuture.complete(util.Arrays.asList[WriteResponse](WriteResponse(input, status = false)))
    }
  }
}
