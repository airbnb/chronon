package ai.chronon.flink

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

object AsyncKVStoreWriter {
  private val kvStoreConcurrency = 10

  def withUnorderedWaits(inputDS: DataStream[KVStore.PutRequest],
    kvStoreWriterFn: RichAsyncFunction[PutRequest, Option[Long]],
    featureGroupName: String,
    timeoutMillis: Long = 1000L,
    capacity: Int = kvStoreConcurrency
  ): DataStream[Option[Long]] = {
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

  private val EXECUTION_CONTEXT_INSTANCE: ExecutionContext = new DirectExecutionContext
}

class AsyncKVStoreWriter(onlineImpl: Api, featureGroupName: String)
  extends RichAsyncFunction[PutRequest, Option[Long]] {

  @transient private var kvStore: KVStore = _

  @transient private var errorCounter: Counter = _
  @transient private var successCounter: Counter = _

  // The context used for the future callbacks
  implicit lazy val executor: ExecutionContext = AsyncKVStoreWriter.EXECUTION_CONTEXT_INSTANCE

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

  override def timeout(input: KVStore.PutRequest, resultFuture: ResultFuture[Option[Long]]): Unit = {
    println(s"Timed out writing to Memento for object: $input")
    errorCounter.inc()
    resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
  }

  override def asyncInvoke(input: PutRequest, resultFuture: ResultFuture[Option[Long]]): Unit = {
    val resultFutureRequested: Future[Seq[Boolean]] = kvStore.multiPut(Seq(input))
    resultFutureRequested.onComplete {
      case Success(l) if l.forall(c => c) =>
        successCounter.inc()
        resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
      case Success(l) if !l.forall(c => c) =>
        // we got a response that was marked as false (write failure)
        errorCounter.inc()
        println(s"Failed to write to KVStore for object: $input")
        resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
      case Failure(exception) =>
        // this should be rare and indicates we have an uncaught exception
        // in the KVStore - we log the exception and skip the object to
        // not fail the app
        errorCounter.inc()
        println(s"Caught exception writing to KVStore for object: $input - $exception")
        resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
    }
  }
}
