package ai.chronon.flink

import ai.chronon.online.{Api, KVStore}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.scala.DataStream

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AsyncKVStoreWriter {
  val kvStoreConcurrency = 10

  def withUnorderedWaits(inputDS: DataStream[KVStore.PutRequest],
    kvStoreWriter: AsyncKVStoreWriter,
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
          kvStoreWriter,
          timeoutMillis,
          TimeUnit.MILLISECONDS,
          capacity
        )
        .uid(s"memento-writer-async-$featureGroupName")
        .name(s"async memento writes for $featureGroupName")
        .slotSharingGroup(featureGroupName)
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

  val EXECUTION_CONTEXT_INSTANCE: ExecutionContext = new DirectExecutionContext
}

class AsyncKVStoreWriter(onlineImpl: Api)
  extends RichAsyncFunction[PutRequest, Option[Long]] {

  @transient private var kvStore: KVStore = _

  // The context used for the future callbacks
  implicit lazy val executor: ExecutionContext = AsyncKVStoreWriter.EXECUTION_CONTEXT_INSTANCE

  override def open(configuration: Configuration): Unit = {
    // TODO: hook up metrics
    kvStore = onlineImpl.genKvStore
  }

  override def timeout(input: KVStore.PutRequest, resultFuture: ResultFuture[Option[Long]]): Unit = {
    // TODO: update metrics
    resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
  }

  override def asyncInvoke(input: PutRequest, resultFuture: ResultFuture[Option[Long]]): Unit = {
    val resultFutureRequested: Future[Seq[Boolean]] = kvStore.multiPut(Seq(input))
    resultFutureRequested.onComplete {
      case Success(l) if l.forall(c => c) =>
        // TODO incr metrics
        resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
      case Success(l) if !l.forall(c => c) =>
        // we got a response that was marked as false (write failure)
        // TODO incr metrics, log errors
        resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
      case Failure(exception) =>
        // this should be rare and indicates we have an uncaught exception
        // in the KVStore - we log the exception and skip the object to
        // not fail the app
        // TODO incr metrics, log errors
        resultFuture.complete(util.Arrays.asList[Option[Long]](input.tsMillis))
    }
  }
}